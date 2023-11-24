# Databricks notebook source
# MAGIC %md 
# MAGIC ## Challenge2. 顧客の生存確率の予測(目安11:35-11:50)
# MAGIC ### 本ノートブックの目的：顧客生存確率を予測するモデルを訓練し、精度を検証する
# MAGIC
# MAGIC その過程でNotebookライクな環境での機械学習の実行を学習します
# MAGIC なお、Databricksでモデルのライフサイクルを管理するための方法は2-4で学習します

# COMMAND ----------

# MAGIC %md ## モデルの訓練

# COMMAND ----------

# DBTITLE 1,クラスルームのセットアップ
# MAGIC %run ./init $mode="1-2"

# COMMAND ----------

# DBTITLE 1,lifetimesライブラリのインストール
# MAGIC %pip install lifetimes==0.10.1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4: モデルのトレーニング(学習)
# MAGIC
# MAGIC
# MAGIC モデルのトレーニングを簡単に行うために、[Pareto/NBDモデル（オリジナルのBTYDモデル）](https://www.jstor.org/stable/2631608)を使用した簡単な演習から始めましょう。<br>
# MAGIC 前のノートブックの最後のセクションで構築されたキャリブレーショングデータとホールドアウトデータセットを使用して、キャリブレーションデータにモデルをフィットさせ、ホールドアウトデータで評価します。

# COMMAND ----------

from lifetimes.fitters.pareto_nbd_fitter import ParetoNBDFitter

# Spark DFをPandas DFに変換
input_pd = spark.table("user_metrics_training").toPandas()

# モデルの学習
model = ParetoNBDFitter(penalizer_coef=0.01)
model.fit( input_pd['frequency_cal'], input_pd['recency_cal'], input_pd['T_cal'])

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC モデルがフィットしたところで、ホールドアウト期間の予測をしてみましょう。次のステップで比較できるように、同じ期間の実績値を取得します。

# COMMAND ----------

# ホールドアウト期間のFrequencyを学習したモデルを使って推測する
frequency_holdout_predicted = model.predict( input_pd['duration_holdout'].values, input_pd['frequency_cal'].values, input_pd['recency_cal'].values, input_pd['T_cal'].values)

# 実際のFrequency
frequency_holdout_actual = input_pd['frequency_holdout']

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC 実際の値と予測値があれば、いくつかの標準的な評価指標を計算することができます。 今後の評価を容易にするために、これらの計算を関数にまとめておきましょう。

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

# 上記で定義した関数`score_model()`を使って、モデルの二乗誤差平均(mse)を計算・表示
print('MSE: {0}'.format(score_model(frequency_holdout_actual, frequency_holdout_predicted, 'mse')))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC モデルを比較するには重要ですが、個々のモデルの全体的な適合性という点では、誤差を示すMSE指標の解釈は少し難しいです。モデルがデータにどれだけフィットしているかをより深く理解するために、いくつかの実測値と予測値の関係を視覚化してみましょう。
# MAGIC
# MAGIC まず、キャリブレーション期間中の購入頻度が、ホールドアウト期間中の実際の頻度（frequency_holdout）および予測頻度（model_predictions）とどのように関連しているかを調べてみます。

# COMMAND ----------

from lifetimes.plotting import plot_calibration_purchases_vs_holdout_purchases

plot_calibration_purchases_vs_holdout_purchases(
  model, 
  input_pd, 
  n=90, 
  **{'figsize':(8,8)}
  )

display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ここでわかることは、キャリブレーション期間の購入回数が多いほど、ホールドアウト期間の平均購入回数も多くなることが予測されます。ただし、キャリブレーション期間の購入回数が多い（60回以上）お客様を考慮すると、実際の値はモデルの予測から大きく乖離します。
# MAGIC
# MAGIC このNotebookの「データ探索」ステップで可視化したチャートによると、このように多くの購入回数を持つ顧客はほとんどいないので、この乖離は頻度の高い範囲のごく限られた事例の結果である可能性があります。より多くのデータがあれば、予測値と実測値がこの高域で再び一致するかもしれません。この乖離が続くようであれば、信頼できる予測ができない顧客エンゲージメント頻度の範囲を示しているのかもしれません。
# MAGIC
# MAGIC 同じメソッドコールを使用して、保留期間中の平均購入回数に対する最終購入からの時間を視覚化することができます。この図では、最終購入からの時間が長くなるにつれて、保留期間中の購入数が減少していることがわかります。 つまり、しばらく会っていないお客様は、すぐには戻ってこない可能性が高いということです。
# MAGIC
# MAGIC
# MAGIC 注: 前回同様、ビジュアライゼーションに集中するため、以下のセルではコードを隠します。関連するPythonロジックを見るには **"Show code"** を使用してください。

# COMMAND ----------

plot_calibration_purchases_vs_holdout_purchases(
  model, 
  input_pd, 
  kind='time_since_last_purchase', 
  n=90, 
  **{'figsize':(8,8)}
  )

display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC このグラフに最終購入時の顧客の経過日数(Age)を入れてみると、顧客ライフサイクルにおける最終購入のタイミングは、顧客の経過日数が大きくなるまで、保留期間の購入回数にはあまり影響しないようです。これは、長く付き合ってくれる顧客は、より頻繁に購入してくれる可能性が高いことを示しています。

# COMMAND ----------

plot_calibration_purchases_vs_holdout_purchases(
  model, 
  input_pd, 
  kind='recency_cal', 
  n=300,
  **{'figsize':(8,8)}
  )

display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ざっと見たところ、このモデルは完璧ではありませんが、いくつかの有用なパターンを捉えていることがわかります。これらのパターンを使って、顧客がエンゲージメントを維持する確率を計算することができます。

# COMMAND ----------

# load df
filtered_pd = spark.table("user_metrics_all").toPandas()

# add a field with the probability a customer is currently "alive"
filtered_pd['prob_alive']=model.conditional_probability_alive(
    filtered_pd['frequency'].values, 
    filtered_pd['recency'].values, 
    filtered_pd['T'].values
    )

filtered_pd.head(10)

# COMMAND ----------

# MAGIC %md 
# MAGIC 予測結果をテーブルに保存する

# COMMAND ----------

predict_df = spark.createDataFrame(filtered_pd)
(predict_df
 .write
 .mode("overwrite")
 .saveAsTable("user_score")
 )

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC 顧客が生存している確率の予測は、モデルをマーケティングプロセスに応用する上で非常に興味深いものとなるでしょう。しかし、モデルの展開を検討する前に、データセットの中で活動が控えめな1人の顧客（CustomerID 12383）の履歴を見て、この確率が顧客の再活動に伴ってどのように変化するかを見てみましょう。

# COMMAND ----------

from lifetimes.plotting import plot_history_alive
import matplotlib.pyplot as plt
from pyspark.sql.functions import to_date

# load orders
orders_pd = (spark
             .table("online_retail")
             .withColumn('InvoiceDateNew', to_date("InvoiceDate", "MM/dd/yy HH:mm"))
             .toPandas())

# clear past visualization instructions
plt.clf()

# customer of interest
CustomerID = 12383

# grab customer's metrics and transaction history
cmetrics_pd = input_pd[input_pd['CustomerID']==CustomerID]
trans_history = orders_pd.loc[orders_pd['CustomerID'] == CustomerID]

# calculate age at end of dataset
days_since_birth = 400

# plot history of being "alive"
plot_history_alive(
  model, 
  days_since_birth, 
  trans_history, 
  'InvoiceDateNew'
  )

display()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC このグラフから、この顧客が2011年1月に最初の購入を行い、その月の後半にリピート購入を行ったことがわかります。その後、約1ヶ月間の活動休止期間があり、この顧客が生存している確率はわずかに低下しましたが、同年3月、4月、6月に購入された際には、顧客が活動していることを示すシグナルが繰り返し発信され、購入する確率が上昇していることが分かります。しかし最後の6月の購入以降、その顧客は取引履歴に現れず、顧客が生きているという確信は、それまでのシグナルを考えると緩やかなペースで低下しています。

# COMMAND ----------


