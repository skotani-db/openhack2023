-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Challenge 4. 増分的に増える注文データから、予測モデルの学習と検証データを生成しよう(目安13:15-13:35)
-- MAGIC ### 本ノートブックの目的：増分的に増える注文データを処理するパイプラインの構築とその実行方法を学ぶ
-- MAGIC
-- MAGIC その過程でDatabricksに関する以下の事項を学習します
-- MAGIC - 増分的な処理を実現する仕組み Auto Loader
-- MAGIC - バッチと増分処理を統合するパイプライン構築機能　Delta Live Tables

-- COMMAND ----------

-- DBTITLE 1,クラスルームセットアップ
-- MAGIC %run ./init $mode="2-2"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## (注意)
-- MAGIC - このDelta Live Tables パイプラインの構築はチーム内で一人が実行するようにします。
-- MAGIC - 結果テーブルのSELECT権限を`account_users`（全ユーザーを意味する）に付与し、後続の分析を行います。

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ### Delta Live Tablesとは
-- MAGIC - SQLやPythonに基づくデータパイプライン構築の宣言型アプローチ
-- MAGIC - データパイプラインをリネージュで確認できる
-- MAGIC - 各テーブルには品質管理の制約を設定することができ、制約違反の件数が出力したり、レコードから削除するなど様々なオプションを利用できる

-- COMMAND ----------

-- MAGIC %python
-- MAGIC slide_id = '1EyfnWydnePJ6g7eq6gaRp4HmDpDoiOEB6Mx_r3ff2F4'
-- MAGIC slide_number = 'id.g29d8b3ef3a0_0_386'
-- MAGIC  
-- MAGIC displayHTML(f'''
-- MAGIC <iframe
-- MAGIC   src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
-- MAGIC   frameborder="0"
-- MAGIC   width="100%"
-- MAGIC   height="600"
-- MAGIC ></iframe>
-- MAGIC ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC このセクションでは、付属しているノートブック（dlt_pipeline）を使って、今までのセクションで構築したメダリオンアーキテクチャのパイプラインを構築します。 　<br>
-- MAGIC 次のレッスンでは、ノートブックの内容について見ていきます。　<br>
-- MAGIC 　<br>
-- MAGIC 1. サイドバーの**Workflows**ボタンをクリックします。
-- MAGIC 1. **Delta Live Tables**タブを選択します。
-- MAGIC 1. **Create Pipeline**をクリックします。
-- MAGIC 1. **Pipeline name**を入力します。これらの名前は一意である必要があるため、**pipeline_your_name**　を入力ください。
-- MAGIC 1. **Product Edition**は**Advanced**のままにします。
-- MAGIC 1. **Pipeline mode**では、**Continuous**を選択します。
-- MAGIC    * このフィールドでは、パイプラインの実行方法を指定します。
-- MAGIC    * **Continuous**パイプラインはデータソースに到着した新しいファイルを継続的に処理し続けます。
-- MAGIC 1. **Source Code**では、ナビゲーターを使って他のノートブックと同じディレクトリにある**dlt_pipeline**という付録のノートブックを探して選択します。
-- MAGIC 1. **Destination**フィールドに、今まで使用したカタログとデータベースの名前 **`team_<x>`** , **`db_hackathon4lakehouse_<username>`** を入力します。
-- MAGIC    * **Storage location**フィールドは指定しない場合、DLTが自動的にディレクトリを生成します。今回は指定しないまま実行します。
-- MAGIC 1. **Cluster** では、**Cluster mode**を **`Fixed Size`** 、ワーカーの数を **`1`** に設定します。
-- MAGIC 1. **Advanced** では、設定を追加から3つのkey-valueを渡します。
-- MAGIC    * **`holdout_days` = `90`** 
-- MAGIC    * **`your_name` = `your_name`** (データベース名で指定した値)
-- MAGIC    * **`your_team` = `team_x`** (チームごとのカタログで指定した値)
-- MAGIC 1. **Create**をクリックして、パイプラインをデプロイします。
-- MAGIC 1. 構成されたDAGやレコードの処理件数を確認しましょう！

-- COMMAND ----------

-- DBTITLE 1,権限の付与(DLT実行者が<catalog>と<database>を置換)
GRANT SELECT ON SCHEMA <catalog>.<database>　 TO `account users`;
