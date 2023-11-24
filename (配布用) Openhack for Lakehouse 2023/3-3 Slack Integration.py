# Databricks notebook source
# MAGIC %md # Slackと連携し自然言語でのデータ問い合わせ
# MAGIC
# MAGIC 先ほどDatabricks側で　SQLDatabaseを使った動作を確認してみました。<br>
# MAGIC 次に下記の図の通り、Slackと連携してユーザーが簡単に利用できるように連携してみたいと思います。
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/webinar/llm/sqldb_slack.png' width='800' />
# MAGIC

# COMMAND ----------

# MAGIC %md # SQL Warehouseの起動と warehouse id の取得
# MAGIC
# MAGIC 外部からアクセスするために、SQLWarehouseを起動し、`host`, `warehouse_id` を取得します。<br>
# MAGIC その他、Personal Access Tokenをユーザー設定から取得してください。
# MAGIC
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/webinar/llm/warehouse_id.png' width='600' >

# COMMAND ----------

# MAGIC %md # コード実行
# MAGIC
# MAGIC 今回はデモや連携テスト目的でDatabricks Cluster上で実行してみます。
# MAGIC
# MAGIC Databricks Runtime 13.1 ML 以降をご利用ください。最小スペックで動作します。
# MAGIC
# MAGIC こちらのサンプルコードは動作を保証するものではありませんのでご注意ください。

# COMMAND ----------

# DBTITLE 1,ライブラリのインストール
!pip install slack_bolt databricks-sql-connector==2.9.3 openai langchain==0.0.205 
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./init $mode="3-3"

# COMMAND ----------

# MAGIC %md ## 環境変数の設定
# MAGIC
# MAGIC ### Azure OpenAI APIの接続情報を取得します
# MAGIC 1. [Azure Portal](https://portal.azure.com/#home)にアクセスします
# MAGIC 1. **リソースグループ** をクリックします
# MAGIC 1. **Hands-on-OpenAI-XXX** のAzure OpenAI Serviceのリソースにアクセスし、`Keys and endpoint`からAPIキーとエンドポイント名を取得します。
# MAGIC  
# MAGIC なお、こちらの接続情報はハッカソン中のみ有効です。
# MAGIC
# MAGIC ### Slackの認証設定
# MAGIC Slack APPの設定やToken取得方法など詳細はこちらをご覧ください。<br>
# MAGIC [Databricksのデータに SQLDatabaseChainを使ってSlackからアクセスしてみた](https://qiita.com/maroon-db/items/ed862592efb65b06d2a9)
# MAGIC 今回は既にSlack app側の設定は完了しておりますので、事前に配布されているSlackトークン一覧を参照しnotebookへ環境変数を設定します。
# MAGIC
# MAGIC ### Databricks Host URLの指定
# MAGIC アクセスしているDatabricksのアプリケーションのURL`https://xxxxxxxx.cloud.databricks.com`から`https://`を除いた`xxxxxxxx.cloud.databricks.com`を入力します。
# MAGIC
# MAGIC ### SQL Warehouseの指定
# MAGIC 左ペインのSQL　Warehouseから、既に起動しているサーバレスウェアハウスを指定し、
# MAGIC `ウェアハウス名 (ID: XXXXXXXXXXX)`からまたSQL WarehouseのIDを発見します。

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


# Slack 連携しない場合は、以下は不要
os.environ['SLACK_BOT_TOKEN'] = "<your-team-bot-token>"
os.environ['SLACK_APP_TOKEN'] = "<your-team-app-token>"

# Databricks Access 情報
os.environ['DATABRICKS_HOST'] = "<your-host-url>"
os.environ['DATABRICKS_WAREHOUSEID'] ="<your-warehouse-id>"
os.environ['DATABRICKS_TOKEN'] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# 変更したか確認する
assert openai.api_key != "<your_key>", "OPENAI_API_KEYを設定してください"
assert openai.api_base != "<your_endpoint>", "エンドポイントを設定してください"
assert os.environ.get("SLACK_BOT_TOKEN") != "<your-team-bot-token>", "Slackのbotトークンを設定してください"
assert os.environ.get("SLACK_APP_TOKEN") != "<your-team-app-token>", "Slackのappトークンを設定してください"
assert os.environ.get("DATABRICKS_HOST") != "<your-host-url>", "DatabricksのHost URLを設定してください"
assert os.environ.get("DATABRICKS_WAREHOUSEID") != "<your-warehouse-id>", "DatabricksのWarehouse IDを設定してください"

# COMMAND ----------

# MAGIC %md ## Databricks SQLwarehouse との連携実装
# MAGIC
# MAGIC 検索対象のテーブルと履歴を保存するテーブル情報を修正してご利用ください。

# COMMAND ----------

import os
import openai
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from langchain import SQLDatabase, SQLDatabaseChain
from langchain.chat_models import ChatOpenAI
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import current_timestamp, date_format

#######################################################
## 今回検索対象とするカタログとスキーマを選択します。       #######
#######################################################

catalog = "samples"
schema = "nyctaxi" 
tables  = ["trips"] 

# LLM
llm = ChatOpenAI(
  deployment_id=deployment_id,
  model_name=model_name,
  temperature=0
  )

## Langchain SQLDatabaseChain
db = SQLDatabase.from_databricks(catalog=catalog, 
                                 schema=schema,
                                 include_tables=tables, 
                                 host = os.environ.get("DATABRICKS_HOST"), 
                                 warehouse_id=os.environ.get("DATABRICKS_WAREHOUSEID"), 
                                 api_token=os.environ.get("DATABRICKS_TOKEN"))
                                 
db_chain = SQLDatabaseChain.from_llm(llm, db, verbose=True)

# Customize for log history to delta table
def delta_chain(input):

  # 保存先テーブルの作成
  d_catalog = "main"
  d_schema = database
  d_table = "query_history"

  spark.sql(f"create catalog if not exists {d_catalog}")
  spark.sql(f"create schema if not exists {d_catalog}.{d_schema}")

  # スキーマを定義
  schema = StructType([
      StructField('query', StringType(), True),
      StructField('result', StringType(), True)
  ])
  
  # SQLDatabaseChainにて問い合わせ
  result = db_chain(input)

  # DataFrameを作成＆保存
  df = spark.createDataFrame([tuple(result.values())], schema=schema)
  df = df.withColumn('time',current_timestamp()).select('time','query','result')
  df.write.mode("append").saveAsTable(f"{d_catalog}.{d_schema}.{d_table}")
  return result['result']


# COMMAND ----------

# MAGIC %md ## Slack のBotと連携実装
# MAGIC
# MAGIC Slack Appや Botの追加についてはこちらの記事をご覧ください。<br>
# MAGIC https://qiita.com/maroon-db/items/ed862592efb65b06d2a9
# MAGIC
# MAGIC 今回は、Bot に対してmention があった際に反応するようにしております。

# COMMAND ----------


app = App(token=os.environ.get("SLACK_BOT_TOKEN") ) 

# mentionされた場合メッセージを読み取ります
@app.event("app_mention")
def respond_to_mention(body, say):
    channel_id = body["event"]["channel"]
    user_id = body["event"]["user"]
    question = body["event"]["text"]

    #response = db_chain.run(question)
    response = delta_chain(question)

    # Craft your response message
    message = f"Answer: {response}"

    # Send the response message
    say(message, channel=channel_id)

@app.event("message")
def handle_message_events(body, logger):
    logger.info(body)

# COMMAND ----------

# MAGIC %md ## コードを外部で実行する場合
# MAGIC
# MAGIC ノートブック以外で実行する際にはこちらのコードを追記して**app.py**として保存した場合、アプリを以下のように実行してください。
# MAGIC
# MAGIC ```python:app.py
# MAGIC
# MAGIC ・・・上記コード・・・
# MAGIC
# MAGIC # 下記コードを追加
# MAGIC if __name__ == "__main__":
# MAGIC     SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
# MAGIC ```
# MAGIC
# MAGIC ` $ python app.py` 

# COMMAND ----------

# MAGIC %md ## ノートブック上で以下を実行すると、Bolt appが起動します。
# MAGIC
# MAGIC 以下の方法は、インタラクティブな開発時での使用が推奨されています。<br>
# MAGIC プロダクションでは、[こちらの記事のコード](https://qiita.com/maroon-db/items/ed862592efb65b06d2a9#3-python%E3%82%B3%E3%83%BC%E3%83%89%E5%AE%9F%E8%A1%8C)を参考に外部で実行し、SQLwarehouseを通じて利用するようにしてください。
# MAGIC
# MAGIC

# COMMAND ----------

from flask import Flask, request

flask_app = Flask("dbchain")
handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))

@flask_app.route('/', methods=['POST'])
def slack_events():
  return handler.start(request)

if __name__ == '__main__':
    handler.start()
    flask_app.run(host="0.0.0.0", port="7777")

