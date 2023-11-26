# Databricks notebook source
# MAGIC %md
# MAGIC ※ 事前に/dbfs/FileStore/ディレクトリに、UCI Machine Learning Repositoryから入手可能な[Online Retailデータセット](https://drive.google.com/file/d/1mPT4SceBwZZYjQg6q8uN6xpDIccyxz-l/view)を配置する必要があります。<br>
# MAGIC GUIでFileStoreにアップロードください。

# COMMAND ----------

# DBTITLE 1,まずはチーム/ユーザー名を入力しましょう
# チーム名 team_xのxを各チームのアルファベット小文字に置換してください
team_name = "team_x"

# ユーザー名
user_name = "your_name"

assert team_name != "team_x", "initノートブックでユーザー名を入力してください"
assert user_name != "your_name", "initノートブックでユーザー名を入力してください"

# COMMAND ----------

# データベース名を変数に指定
catalog = team_name
database_name = "db_hackathon4lakehouse"

dbutils.widgets.text("mode", "init")
mode = dbutils.widgets.get("mode")

database = f"{database_name}_{user_name}"

# 作業領域のディレクトリ
data_path = f'/FileStore/db_hackathon4lakehouse_2023/{user_name}'

# COMMAND ----------

if mode == "init":

    # カタログの準備
    spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog}')
    spark.sql(f'USE CATALOG {catalog}')

    # データベースの準備
    spark.sql(f'DROP DATABASE IF EXISTS {database} CASCADE')
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {database}')

    print('\n')

# データベースのデフォルトをセット
spark.sql(f'USE CATALOG {catalog}')
spark.sql(f"USE DATABASE {database}")
print(f"catalog  : {spark.sql('SELECT current_catalog()').first()[0]}")
print(f"database  : {spark.sql('SELECT current_database()').first()[0]}")
spark.sql("SET spark.sql.legacy.timeParserPolicy=LEGACY;")

if mode == "2-1":
    volume = f"volume_{user_name}"

if mode == "2-2":
    print(f"pipeline name  :  pipeline_{user_name}")


# if mode == "cleanup":
#     spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
#     dbutils.fs.rm(data_path, True)
