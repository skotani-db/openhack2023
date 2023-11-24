# Databricks notebook source
# MAGIC %md 
# MAGIC ## Challenge3. リアルタイムの注文トランザクションデータの管理(目安13:15-13:25)
# MAGIC ### 本ノートブックの目的：増分的に増えるトランザクションデータをカタログで一元的に管理する方法を学ぶ
# MAGIC
# MAGIC その過程でDatabricksに関する以下の事項を学習します
# MAGIC - DataとAIの統合的なガバナンスソリューション Unity Catalog
# MAGIC - Unity Catalog Volumeを用いた非構造化データのガバナンス

# COMMAND ----------

# DBTITLE 1,セットアップ
# MAGIC %run ./init $mode="2-1"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 人工データの生成
# MAGIC このステップでは、人工的にOnline Retailのデータを日付ごとに分けたcsvファイルの形式で書き込みを行います。通常、このようなデータのランディングゾーンはデータレイクになりますが、ガバナンスが複雑になることが課題でした。DatabricksではUnity Catalog Volumeによって、オブジェクトストレージをファイルシステムのように扱うことができ、アクセスコントロールをDatabricksのアイデンティティを元に行うことができます。
# MAGIC

# COMMAND ----------

slide_id = '1EyfnWydnePJ6g7eq6gaRp4HmDpDoiOEB6Mx_r3ff2F4'
slide_number = 'id.g29db854fa67_0_337'
 
displayHTML(f'''
<iframe
  src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
  frameborder="0"
  width="100%"
  height="600"
></iframe>
''')

# COMMAND ----------

# DBTITLE 1,トランザクションデータと日付のリストを作成
from pyspark.sql.functions import to_date

df = (spark
      .table("online_retail")
      .withColumn("InvoiceDateNew", to_date("InvoiceDate", "MM/dd/yy HH:mm"))
      )

date_list = (df
             .select("InvoiceDateNew")
             .distinct()
             .toPandas()
             .sort_values(by="InvoiceDateNew")
             )

# COMMAND ----------

# DBTITLE 1,Unity Catalog Volumeを作成する
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume}")

# COMMAND ----------

# DBTITLE 1,200日分まで、1秒ごとに1日分のトランザクションを書き込む(データが増えている間にカタログで確認する)
from time import sleep
from pyspark.sql.functions import col

df_cached = (df
             .drop("_rescued_data")
             .cache()
)

for d in date_list[:200]["InvoiceDateNew"]:
  pdf = (df_cached
        .where(col("InVoiceDateNew") == d)
        .drop("InVoiceDateNew")
        .toPandas()
        )
  pdf.to_csv(f"/Volumes/{catalog}/{database}/{volume}/orders_{d}.csv", index = False)
  print(f"write records on {str(d)}")

# COMMAND ----------

# DBTITLE 1,※新データ更新用　200日以降のデータを書き込む
# for d in date_list[200:]["InvoiceDateNew"]:
#   pdf = (df_cached
#         .where(col("InVoiceDateNew") == d)
#         .drop("InVoiceDateNew")
#         .toPandas()
#         )
#   pdf.to_csv(f"/Volumes/{catalog}/{database}/{volume}/orders_{d}.csv", index = False)
#   print(f"write records on {str(d)}")
