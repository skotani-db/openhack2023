# Databricks notebook source
# MAGIC %md # SQLDatabaseChainを使った自然言語でのデータ問い合わせ
# MAGIC
# MAGIC 今回はLangchainの SQLDatabaseChainというChainを利用して、Databricks内のデータに問い合わせをしてみます。<br>
# MAGIC LLMとしては、OpenAIのモデルを利用しております。（他にAzure OpenAIにも対応）
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/webinar/llm/llm-database-1.png' width='800' />
# MAGIC
# MAGIC Databricks Runtime 13.1 ML 以降をご利用ください。

# COMMAND ----------

# DBTITLE 1,クラスルームセットアップ
!pip install databricks-sql-connector==2.9.3 openai langchain==0.0.205 
dbutils.library.restartPython()

# COMMAND ----------

import os
import openai
from langchain import SQLDatabase, SQLDatabaseChain
from langchain.chat_models import ChatOpenAI

# COMMAND ----------

# MAGIC %md ## Azure OpenAI APIの接続情報を取得します
# MAGIC 1. [Azure Portal](https://portal.azure.com/#home)にアクセスします
# MAGIC 1. **リソースグループ** をクリックします
# MAGIC 1. **Hands-on-OpenAI-XXX** のAzure OpenAI Serviceのリソースにアクセスし、`Keys and endpoint`からAPIキーとエンドポイント名を取得します。
# MAGIC  
# MAGIC なお、こちらの接続情報はハッカソン中のみ有効です。

# COMMAND ----------

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

# MAGIC %md ## SQLDatabaseChainの設定
# MAGIC
# MAGIC 今回対象とするカタログやスキーマ、テーブルなどを指定します。<br><br>
# MAGIC SQLDatabaseChainの使い方については、こちらをご覧ください。<br>
# MAGIC https://python.langchain.com/docs/ecosystem/integrations/databricks
# MAGIC
# MAGIC Qiita: [(翻訳) LangChainのSQLDatabaseChain](https://qiita.com/taka_yayoi/items/7759f31341b91bc707f4)

# COMMAND ----------

#######################################################
## 今回検索対象とするカタログとスキーマを選択します。   #######
#######################################################

catalog = "samples"
schema = "nyctaxi" 
tables  = ["trips"] 

# # LLM. 今回は ChatGPT3.5を利用
llm = ChatOpenAI(
    deployment_id=deployment_id,
    model_name=model_name,
    temperature=0,
)

# DB 
db = SQLDatabase.from_databricks(catalog="samples", schema="nyctaxi")
db_chain = SQLDatabaseChain.from_llm(llm, db, verbose=True)

# COMMAND ----------

# MAGIC %md ### 動作確認
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,その前に対象データを確認
display(spark.table(f"{catalog}.{schema}.trips"))

# COMMAND ----------

db_chain.run("18時以降の平均の運賃はいくらですか?")

# COMMAND ----------

db_chain.run("乗車の平均時間は？")

# COMMAND ----------

# MAGIC %md ## 検索履歴を保存するようにカスタマイズ
# MAGIC
# MAGIC ユーザーがどのような問い合わせをしたのかを履歴としてDelta Tableに保存するように、カスタマイズします。<br>
# MAGIC 保存するカタログやスキーマ、テーブル名などを変更してからご利用ください。

# COMMAND ----------

# MAGIC %run ./init $mode="3-2"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import current_timestamp, date_format

# 保存先テーブルの作成
d_catalog = catalog
d_schema = database
d_table = "query_history"

def delta_chain(input):

  # スキーマを定義
  schema = StructType([
      StructField('query', StringType(), True),
      StructField('result', StringType(), True)
  ])
  
  # SQLDatabaseChainにて問い合わせ
  result = db_chain(input)

  # DataFrameを作成＆保存
  df = spark.createDataFrame( [tuple(result.values())], schema=schema)
  df = df.withColumn('time',current_timestamp()).select('time','query','result')
  df.write.mode("append").saveAsTable(f"{d_catalog}.{d_schema}.{d_table}")
  return result['result']


# COMMAND ----------

# DBTITLE 1,動作テスト
delta_chain("18時以降の平均の運賃はいくらですか?")

# COMMAND ----------

# DBTITLE 1,history tableの確認
display(spark.table(f"{d_catalog}.{d_schema}.{d_table}"))
