# Databricks notebook source
# MAGIC %md 
# MAGIC ## Challenge 6. 顧客生存予測モデルのライフサイクルを管理する (目安13:50-14:10)
# MAGIC ### 本ノートブックの目的：モデルのバージョンや実験管理を行い、データの変化に伴いモデルの精度は劣化に適応するための方法を学ぶ
# MAGIC
# MAGIC その過程でDatabricksに関する以下の事項を学習します
# MAGIC - Mlflow  Experimentによるモデルの実験管理
# MAGIC - モデルの精度検証を行うための画像データをArtifactとして保存する方法
# MAGIC - Mlflow Modelsによるモデルのバージョン管理

# COMMAND ----------

# DBTITLE 1,クラスルームのセットアップ
# MAGIC %run ./init $mode="2-4"

# COMMAND ----------

# DBTITLE 1,lifetimesライブラリのインストール
# MAGIC %pip install lifetimes==0.10.1

# COMMAND ----------

# MAGIC %md
# MAGIC ### モデルのトレーニング(学習)
# MAGIC
# MAGIC
# MAGIC モデルのトレーニングを簡単に行うために、[パレート/NBDモデル（オリジナルのBTYDモデル）](https://www.jstor.org/stable/2631608)を使用した簡単な演習から始めましょう。前のノートブックの最後のセクションで構築されたキャリブレーショングデータとホールドアウトデータセットを使用して、キャリブレーションデータにモデルをフィットさせ、ホールドアウトデータで評価します。

# COMMAND ----------

import numpy as np

def score_model(actuals, predicted, metric='mse'):
  # metric名を小文字に統一して扱う
  metric = metric.lower()
  
  # metric引数が、二乗誤差平均(mse)と標準偏差(rmse)の場合
  if metric=='mse' or metric=='rmse':
    val = np.sum(np.square(actuals-predicted))/actuals.shape[0]
    if metric=='rmse':
        val = np.sqrt(val)
  
  # metric引数が、平均絶対誤差(mae)の場合
  elif metric=='mae':
    np.sum(np.abs(actuals-predicted))/actuals.shape[0]
  
  else:
    val = None
  
  return val

# COMMAND ----------

# DBTITLE 1,LTVモデルの宣言(mlflow pyfuncモデル)
from mlflow import pyfunc
class LTVModel(pyfunc.PythonModel):
  def __init__(self, model):
    self.model = model

# COMMAND ----------

# MAGIC %md
# MAGIC ## 実験の管理
# MAGIC 以下のセルでは実際にモデルを訓練した結果をMlflow Experimentに登録しています。<br>
# MAGIC 実験をExperimentに登録するためには、1-2 Data Scienceで行った訓練用のコードを`with mlflow.start_run() as run:`で開始するだけです。<br>
# MAGIC Experimentには以下の文法でモデルだけではなく、画像や wheel等の成果物を記録することが可能です。<br>
# MAGIC `mlflow.log_artifact(pic1_name)`

# COMMAND ----------

from lifetimes.fitters.pareto_nbd_fitter import ParetoNBDFitter
from lifetimes.plotting import plot_calibration_purchases_vs_holdout_purchases
import mlflow

# Spark DFをPandas DFに変換
input_pd = spark.table("user_metrics_training").toPandas()

# モデルの学習
with mlflow.start_run() as run:
  ltv_model = LTVModel(model=ParetoNBDFitter(penalizer_coef=0.01))
  ltv_model.model.fit(input_pd['frequency_cal'], input_pd['recency_cal'], input_pd['T_cal'])
  mlflow.pyfunc.log_model(artifact_path="model", python_model=ltv_model)

  # ホールドアウト期間のFrequencyを学習したモデルを使って推測する
  frequency_holdout_predicted = ltv_model.model.predict(input_pd['duration_holdout'].values, input_pd['frequency_cal'].values, input_pd['recency_cal'].values, input_pd['T_cal'].values)

  # 実際のFrequency
  frequency_holdout_actual = input_pd['frequency_holdout']
  
  # 上記で定義した関数`score_model()`を使って、モデルの二乗誤差平均(mse)を計算
  mlflow.log_metric("MSE", score_model(frequency_holdout_actual, frequency_holdout_predicted, 'mse'))

  # キャリブレーション期間中の購入頻度が、ホールドアウト期間中の実際の頻度および予測頻度とどのように関連しているかを調べてみる
  pic1_name = "Actual_Purchase_vs_Predicted_Purchase.png"
  pic1 = plot_calibration_purchases_vs_holdout_purchases(
    ltv_model.model, 
    input_pd, 
    n=90, 
    **{'figsize':(8,8)}
    )
  pic1.get_figure().savefig(pic1_name)
  mlflow.log_artifact(pic1_name)

  # 保留期間中の平均購入回数に対する最終購入からの時間を視覚化
  pic2_name = "Actual_Purchase_in_Holdout_period_vs_Predicted_Purchase.png"
  pic2 = plot_calibration_purchases_vs_holdout_purchases(
    ltv_model.model,
    input_pd, 
    kind='time_since_last_purchase', 
    n=90, 
    **{'figsize':(8,8)}
    )
  pic2.get_figure().savefig(pic2_name)
  mlflow.log_artifact(pic2_name)

  # 顧客ライフサイクルにおける最終購入のタイミング
  pic3_name = "Actual_Purchase_duction_vs_Ages.png"
  pic3 = plot_calibration_purchases_vs_holdout_purchases(
    ltv_model.model, 
    input_pd, 
    kind='recency_cal', 
    n=300,
    **{'figsize':(8,8)}
    )
  pic2.get_figure().savefig(pic3_name)
  mlflow.log_artifact(pic3_name)

# COMMAND ----------

# load df
filtered_pd = spark.table("user_metrics_all").toPandas()

# add a field with the probability a customer is currently "alive"
filtered_pd['prob_alive'] = ltv_model.model.conditional_probability_alive(
    filtered_pd['frequency'].values, 
    filtered_pd['recency'].values, 
    filtered_pd['T'].values
    )

# filtered_pd.head(10)

predict_df = spark.createDataFrame(filtered_pd)
(predict_df
 .write
 .mode("overwrite")
 .saveAsTable("user_score")
 )

# COMMAND ----------

# MAGIC %md ## mlflowで作成したExperimentsをクリックします
# MAGIC
# MAGIC <img src='https://github.com/naoyaabe-db/aws-databricks-hackathon-jp-20221006/raw/main/images/mlflow_experiments2.jpg' />

# COMMAND ----------

# MAGIC %md ## mlflowから作成したモデルをModel registryに登録
# MAGIC <br>
# MAGIC </br>
# MAGIC <img src='https://github.com/naoyaabe-db/aws-databricks-hackathon-jp-20221006/raw/main/hackathon-for-lakehouse/mlflow-first.png' />
# MAGIC <br>
# MAGIC </br>
# MAGIC
# MAGIC **ご自身のお名前をいれたmodel名にしてください** 
# MAGIC
# MAGIC <img src='https://github.com/naoyaabe-db/aws-databricks-hackathon-jp-20221006/raw/main/images/register_model.jpg' />
# MAGIC <br>
# MAGIC </br>
# MAGIC
# MAGIC **赤枠をクリックしてください** 
# MAGIC <img src='https://github.com/naoyaabe-db/aws-databricks-hackathon-jp-20221006/raw/main/images/regist_model2.jpg' />
# MAGIC <br>
# MAGIC </br>
# MAGIC
# MAGIC **Transit to productionをクリックします** 
# MAGIC <img src='https://github.com/naoyaabe-db/aws-databricks-hackathon-jp-20221006/raw/main/hackathon-for-lakehouse/mlflow-second.png' />
# MAGIC <br>
# MAGIC </br>
# MAGIC **この作業を実施することで、DatabricksのModel Registryに登録が行われ、mlflowのAPIやsparkから呼び出すことが可能になります。modelの確認はサイドバーからでも確認可能です**
