# Databricks notebook source
# MAGIC %md 
# MAGIC ## Challenge 5. ビジネスビューを作成して、Databricks SQLからクエリする(目安13:35-13:50)
# MAGIC ### 本ノートブックの目的：定点分析に活用するゴールドテーブルを定義し、DatabricksのDWH機能で検索を行う
# MAGIC
# MAGIC この章では、以下の三種類のゴールドテーブル（集計等を含むビジネスビュー）を作成します。
# MAGIC - 1日ごとのユニークなトランザクション数
# MAGIC - 月ごとのユニークなトランザクション数
# MAGIC - 顧客ごとの取引件数の分布
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 注意　
# MAGIC ### クエリは左ペインからSQLエディタを開き、SQL Warehouseで実行しましょう！

# COMMAND ----------

# DBTITLE 1,セットアップ
# MAGIC %run ./init $mode="2-3"

# COMMAND ----------

# DBTITLE 1,使用するDBの指定とデータマートの定義
# MAGIC %sql 
# MAGIC
# MAGIC -- チームメンバーのデータベースを参照する場合、こちらで指定する
# MAGIC USE DATABASE db_db_hackathon4lakehouse_your_name;
# MAGIC
# MAGIC -- 1日ごとのユニークなトランザクション数
# MAGIC -- SQL Warehouse で実行ください
# MAGIC CREATE OR REPLACE TABLE num_transaction_day AS
# MAGIC SELECT 
# MAGIC   InvoiceDateFmt as InvoiceDate,
# MAGIC   COUNT(DISTINCT InvoiceNo) as Transactions
# MAGIC FROM dlt_online_retail
# MAGIC GROUP BY InvoiceDateFmt
# MAGIC ORDER BY InvoiceDate;
# MAGIC
# MAGIC -- 月ごとのユニークなトランザクション数
# MAGIC CREATE OR REPLACE TABLE num_transaction_month AS
# MAGIC SELECT
# MAGIC   TRUNC(InvoiceDateFmt, "MM") as InvoiceMonth,
# MAGIC   COUNT(DISTINCT InvoiceNo) as Transactions
# MAGIC FROM dlt_online_retail
# MAGIC GROUP BY TRUNC(InvoiceDateFmt, "MM")
# MAGIC ORDER BY InvoiceMonth;
# MAGIC
# MAGIC -- 顧客ごとの取引件数の分布
# MAGIC -- 当日取引を1つの取引として考慮した場合
# MAGIC CREATE OR REPLACE TABLE purchase_distrbution AS
# MAGIC SELECT
# MAGIC   x.Transactions,
# MAGIC   COUNT(x.*) as Occurances
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     CustomerID,
# MAGIC     COUNT(DISTINCT InvoiceDateFmt) as Transactions
# MAGIC   FROM dlt_online_retail
# MAGIC   WHERE CustomerID IS NOT NULL
# MAGIC   GROUP BY CustomerID
# MAGIC   ) x
# MAGIC GROUP BY 
# MAGIC   x.Transactions
# MAGIC ORDER BY
# MAGIC   x.Transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Databricks SQL ダッシュボード
# MAGIC
# MAGIC 1. 上記の実行が終わったら、Databricks ダッシュボードを作成します。<br>
# MAGIC その際に右上のウェアハウスがサーバレスとなっていることを確認しましょう！<br>
# MAGIC 詳細なステップについては[こちらの動画](https://youtu.be/u7yS7_Ovh-w)を確認ください。
# MAGIC
# MAGIC
# MAGIC 1. 2-1 Generate Transaction DataのCmd 8 「※新データ更新用　200日以降のデータを書き込む」というセルのコメントアウトを削除し、実行します。　Online Retailの200日以後のデータが到着します。
# MAGIC
# MAGIC 1. ダッシュボードの画面に戻り、 **Refresh** を押して新規データが反映されていることを確認します。
