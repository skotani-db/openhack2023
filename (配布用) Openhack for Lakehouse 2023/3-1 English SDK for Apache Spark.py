# Databricks notebook source
# MAGIC %md 
# MAGIC ## Challenge 7. The hottest programming language is Japanese. (目安14:10-14:20)
# MAGIC ### 本ノートブックの目的：Azure Open AI Serviceと連携して、自然言語でデータ解析のためのクエリを行う
# MAGIC
# MAGIC その過程でDatabricksに関する以下の事項を学習します
# MAGIC - English SDK for Apache Sparkの設定方法
# MAGIC - Databricks Notebookへクエリ結果とビジュアライズを出力する方法

# COMMAND ----------

# DBTITLE 1,クラスルームセットアップ
# MAGIC %run ./init $mode="3"

# COMMAND ----------

# MAGIC %md
# MAGIC English SDK for Apache Spark は、英語だけでなく自然言語の命令を受け取り、それらを Spark オブジェクトにコンパイルします。 Sparkをよりユーザーフレンドリーでアクセスしやすくし、データから知見を抽出することに集中できるようになります。
# MAGIC
# MAGIC このセクションには、Databricks Python ノートブックを使用して Apache Spark 用の英語の SDK を呼び出す方法を説明する例が含まれています。 この例では、プレーンな英語の質問を使用して、Databricks ワークスペースのテーブルに対して SQL クエリーを実行するようにEnglish SDK for Apache Sparkをガイドします。

# COMMAND ----------

# DBTITLE 1,English SDK for Apache Spark（pyspark-ai）のインストール
# MAGIC %pip install pyspark-ai --upgrade
# MAGIC %pip install sqlalchemy==2.0.12
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Azure OpenAI APIの接続情報を取得します
# MAGIC 1. [Azure Portal](https://portal.azure.com/#home)にアクセスします
# MAGIC 1. **リソースグループ** をクリックします
# MAGIC 1. **Hands-on-OpenAI-XXX** のAzure OpenAI Serviceのリソースにアクセスし、`Keys and endpoint`からAPIキーとエンドポイント名を取得します。
# MAGIC  
# MAGIC なお、こちらの接続情報はハッカソン中のみ有効です。

# COMMAND ----------

# DBTITLE 1,API の環境設定を変数にセット
import os
import openai

# Azure Open AI のキー
os.environ["OPENAI_API_KEY"] = "<your_key>"
openai.api_key = os.getenv("OPENAI_API_KEY")
 
# `Azure`固定
openai.api_type = "azure"
 
# Azure Open AI のエンドポイント
openai.api_base = "<your_endpoint>"

# Azure Docs 記載の項目
openai.api_version = "2023-05-15"

# デプロイ名
deployment_id = "gpt-35-turbo"
 
# デプロイしたモデル名
model_name = "gpt-35-turbo"

# 変更したか確認する
assert openai.api_key != "<your_key>", "OPENAI_API_KEYを設定してください"
assert openai.api_base != "<your_endpoint>", "エンドポイントを設定してください"

# COMMAND ----------

# DBTITLE 1,English SDK for Apache Spark（pyspark-ai）の有効化
from langchain.chat_models import ChatOpenAI
from pyspark_ai import SparkAI
 
llm = ChatOpenAI(
    deployment_id=deployment_id,
    model_name=model_name,
    temperature=0,
)
 
spark_ai = SparkAI(llm=llm, verbose=True)
spark_ai.activate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 変換処理
# MAGIC `df.ai.transform("クエリ")`によって、sparkのデータフレームに対してクエリで命令された変換処理を実施し、Sparkデータフレームを出力します。
# MAGIC

# COMMAND ----------

# DBTITLE 1,全データ取得します
user_metrics__spark_df = spark.read.table("user_score")
user_metrics__spark_ai_df = spark_ai.transform_df(user_metrics__spark_df, "全データを取得します")
 
user_metrics__spark_ai_df.display()

# COMMAND ----------

# DBTITLE 1,ユーザー抽出
spark_ai.transform_df(user_metrics__spark_df, "生存確率が0.99以上のユーザーを抽出します").display()

# COMMAND ----------

# DBTITLE 1,利用頻度の平均値の取得
spark_ai.transform_df(user_metrics__spark_df, "ユーザーのサービス利用の頻度の平均値を取得しなさい").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 描画処理
# MAGIC `df.ai.plot("クエリ")`によって、sparkのデータフレームに対してクエリで命令された描画処理をpythonのコードで生成します。生成されたコードはDatabricksクラスターで実行され、Notebookに出力されます。

# COMMAND ----------

# DBTITLE 1,ヒストグラムの描画
user_metrics__spark_df.ai.plot("生存確率のヒストグラムを描画しなさい")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 以下のセルで独自のクエリをしてみましょう！

# COMMAND ----------

# df.ai.transform("クエリ")
# df.ai.plot("クエリ")
