# Databricks notebook source
# MAGIC %md 
# MAGIC ## Challenge1. 小売企業のオンラインストアの基礎分析(目安11:15-11:35)
# MAGIC ### 本ノートブックの目的：与えられたECのデータから、顧客の取引状況の理解を深める
# MAGIC
# MAGIC その過程でDatabricksに関する以下の事項を学習します
# MAGIC - DatabricksノートブックにおけるPython/SQLを使ったテーブルデータの操作
# MAGIC - テーブルデータの可視化
# MAGIC - カタログに登録することによるデータの永続化
# MAGIC - (Nice to have) SparkのPythonとSQL、それぞれの文法を理解する。実装パターンをいくつか試しているだけなので、深入りしなくても問題ありません！

# COMMAND ----------

# MAGIC %md ##将来のカスタマーエンゲージメントの確率を算出する
# MAGIC
# MAGIC サブスクリプション型ではない小売のビジネスモデルでは、顧客は長期的なコミットメントなしに離反と購買を繰り返します。
# MAGIC そういった状況で、ある顧客が将来的に戻ってくるかどうかを判断することは非常に困難です。効果的なマーケティングキャンペーンを実施するためには、顧客が再来店する可能性を判断し、優先度の高い顧客を見つけることが重要です。
# MAGIC
# MAGIC
# MAGIC 一度退会した可能性の高いお客様に再び来店していただくためには、異なるメッセージングやプロモーションが必要になる場合があります。また、一度ご来店いただいたお客様には、当社でのご購入の幅や規模を拡大していただくためのマーケティング活動に、より反応して頂ける可能性があります。お客様が将来的に関与する可能性についてどのような位置にいるかを理解することは、お客様に合わせたマーケティング活動を行う上で非常に重要です。
# MAGIC
# MAGIC Peter Faderらが提唱したBTYD（Buy Till You Die）モデルは、以下2つの基本的な顧客指標を利用して、将来の再購入の確率を導き出すものです。<br>
# MAGIC 1. **顧客が最後に購入した時からの経過日数(Recency)** 
# MAGIC 1. **顧客の生涯におけるリピート取引の頻度(Frequency)** 
# MAGIC
# MAGIC これらのモデルの背後にある数学はかなり複雑ですが、ありがたいことに[lifetimes](https://pypi.org/project/Lifetimes/)ライブラリにまとめられているため、従来の企業でも採用しやすくなっています。このノートの目的は、これらのモデルを顧客の取引履歴にどのように適用するか、またマーケティングプロセスにどのように統合するかを検討することです。

# COMMAND ----------

# MAGIC %md ###Step 1: 環境構築
# MAGIC
# MAGIC このステップでは、 **ハッカソンで用いる参加者ごとのデータベース** をセットアップし、必要なモジュールを用意します。Chapter 1: DE&DSの配下にある **init** ノートブック のコマンド2にご自身の"firstname_lastname"を入力ください。
# MAGIC
# MAGIC ノートブックの以下のセルを実行するには、Databricks ML Runtime v13.3+以降のクラスタに接続する必要があります。
# MAGIC
# MAGIC Databricks Runtimeは、多くのライブラリがプリインストールされていますが、このNotebookでは追加で以下のPythonライブラリを使用しているので、別途インストールする必要があります。
# MAGIC
# MAGIC * lifetimes==0.10.1

# COMMAND ----------

# DBTITLE 1,クラスルームのセットアップ
# MAGIC %run ./init $mode="init"

# COMMAND ----------

# DBTITLE 1,lifetimesライブラリのインストール
# MAGIC %pip install lifetimes==0.10.1

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC 今回使うデータセット「Online Retail」の各レコードは、販売取引のアイテムを表しています。それぞれのフィールドは以下の通りです。
# MAGIC
# MAGIC | Field | Description |
# MAGIC |-------------:|-----:|
# MAGIC |InvoiceNo|各トランザクションに一意に割り当てられた6桁の整数|
# MAGIC |StockCode|それぞれの製品に一意に割り当てられた5桁の整数|
# MAGIC |Description|プロダクト名(アイテム名)|
# MAGIC |Quantity|トランザクションあたりの数量|
# MAGIC |InvoiceDate|Invoiceの日時(`mm/dd/yy hh:mm`フォーマット)|
# MAGIC |UnitPrice|製品単価 (£, ポンド)|
# MAGIC |CustomerID| 顧客に一意に割り当てられた5桁の整数|
# MAGIC |Country|顧客の居住国|
# MAGIC
# MAGIC これらのフィールドのうち、今回の作業で特に注目すべきものは、取引を識別するInvoiceNo、取引の日付を識別するInvoiceDate、複数の取引で顧客を一意に識別するCustomerIDです。

# COMMAND ----------

# MAGIC %md ###Step 2: データセットの探索
# MAGIC
# MAGIC SQLを使ってデータを探索してみましょう。まずonline_retailテーブルを作成してクエリします。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- online_retail.csvを読み込んでテーブル化します
# MAGIC CREATE
# MAGIC OR REPLACE TABLE online_retail AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   read_files(
# MAGIC     "dbfs:/FileStore/online_retail.csv",
# MAGIC     format => 'csv',
# MAGIC     header => true
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   online_retail

# COMMAND ----------

# MAGIC %md
# MAGIC 以下の分析では、 online_retailテーブルを深掘りしていきます。
# MAGIC
# MAGIC * 最初の取引が2010年12月1日
# MAGIC * 最後の取引が2011年12月9日
# MAGIC * データセットの期間は1年を少し超えている
# MAGIC
# MAGIC ことがわかります。

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   online_retail

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC
# MAGIC まず、1日のトランザクション数を見ると、このオンライン小売業者の1日の取引数にはばらつきがあることがわかります。<br>
# MAGIC クエリ結果の**テーブル**タブ横の **+** タブをクリックすると、結果を元にビジュアライゼーションやプロファイルを作成することができます。

# COMMAND ----------

# DBTITLE 1,1日のトランザクション数
# MAGIC %sql
# MAGIC -- 1日ごとのユニークなトランザクション数
# MAGIC SELECT
# MAGIC   TO_DATE(InvoiceDate, "MM/dd/yy HH:mm") as InvoiceDateOnly,
# MAGIC   COUNT(DISTINCT InvoiceNo) as Transactions
# MAGIC FROM
# MAGIC   online_retail
# MAGIC GROUP BY
# MAGIC   InvoiceDateOnly
# MAGIC ORDER BY
# MAGIC   InvoiceDateOnly

# COMMAND ----------

# MAGIC %md
# MAGIC 月別のトランザクションを要約することで、このグラフを少し滑らかにすることができます。2011年12月は9日間しかなかったので、先月の売上減少のグラフはほとんど無視してよいでしょう。

# COMMAND ----------

# DBTITLE 1,月次のトランザクション数
# MAGIC %sql
# MAGIC -- 月ごとのユニークなトランザクション数
# MAGIC SELECT
# MAGIC   TRUNC(TO_DATE(InvoiceDate, "MM/dd/yy HH:mm"), "MM") as InvoiceMonth,
# MAGIC   COUNT(DISTINCT InvoiceNo) as Transactions
# MAGIC FROM
# MAGIC   online_retail
# MAGIC GROUP BY
# MAGIC   InvoiceMonth
# MAGIC ORDER BY
# MAGIC   InvoiceMonth;

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC データのある1年強の期間では、4,000人以上のユニークなお客様がいます。
# MAGIC これらのお客様が生み出したユニークな取引は約2万2千件です。

# COMMAND ----------

# DBTITLE 1,顧客数と取引回数の総計
# MAGIC %sql
# MAGIC -- unique customers and transactions
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT CustomerID) as Customers,
# MAGIC   COUNT(DISTINCT InvoiceNo) as Transactions
# MAGIC FROM
# MAGIC   online_retail
# MAGIC WHERE
# MAGIC   CustomerID IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 簡単な割り算で、顧客一人当たり、平均して約5件の取引を担当していることがわかります。しかし、これでは顧客の活動を正確に表すことはできません。
# MAGIC
# MAGIC 単純平均ではなく、顧客ごとにユニークな取引をカウントし、その値の頻度を調べてみると、ほとんどのお客様が1回限りの取引を行なっていると分かります。そこから取引回数が増えるにつれて、顧客数は減っていく傾向にあります。
# MAGIC
# MAGIC 統計的なモデルでは、リピート購入回数の分布は、リピート購入回数の分布は、そこから負の二項分布という確率分布にしたがい単調に減少していくと仮定されます。（負の二項分布(Negative Binomial Distribution)は、ほとんどのBTYDモデルの名前に含まれるNBDの頭文字となっています）

# COMMAND ----------

# DBTITLE 1,各顧客の取引回数のヒストグラム(分布)
# MAGIC %sql
# MAGIC -- 顧客ごとの取引回数の分布
# MAGIC SELECT
# MAGIC   x.Transactions,
# MAGIC   COUNT(x.*) as Occurrences
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       CustomerID,
# MAGIC       COUNT(DISTINCT InvoiceNo) as Transactions
# MAGIC     FROM
# MAGIC       online_retail
# MAGIC     WHERE
# MAGIC       CustomerID IS NOT NULL
# MAGIC     GROUP BY
# MAGIC       CustomerID
# MAGIC   ) x
# MAGIC GROUP BY
# MAGIC   x.Transactions
# MAGIC ORDER BY
# MAGIC   x.Transactions;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC リピート購入のお客様に焦点を当てて、購入イベント間の日数の分布を調べることができます。ここで重要な事実は、ほとんどのお客様が購入後2～3ヶ月以内にサイトに戻ってきていることです。それ以上の期間が空くこともありますが、再購入までの期間が長いお客様はかなり少ないです。
# MAGIC
# MAGIC このことは、BYTDモデルの文脈で理解することが重要です。つまり、最後にお客様にお会いしてからの時間は、お客様が再来店されるかどうかを判断するための重要な要素であり、お客様が最後に購入されてからの時間が長くなればなるほど、再来店の確率は下がっていきます。

# COMMAND ----------

# DBTITLE 1,リピート時の取引の間隔(日数)
# MAGIC %sql
# MAGIC -- 顧客別平均購入イベント間隔日数分布
# MAGIC WITH CustomerPurchaseDates AS (
# MAGIC   SELECT
# MAGIC     DISTINCT CustomerID,
# MAGIC     TO_DATE(InvoiceDate, "MM/dd/yy HH:mm") as InvoiceDate
# MAGIC   FROM
# MAGIC     online_retail
# MAGIC   WHERE
# MAGIC     CustomerId IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   -- 顧客別平均購入イベント間隔日数
# MAGIC   AVG(
# MAGIC     DATEDIFF(a.NextInvoiceDate, a.InvoiceDate)
# MAGIC   ) as AvgDaysBetween
# MAGIC FROM
# MAGIC   (
# MAGIC     -- 顧客別購入イベントと次回購入イベント
# MAGIC     SELECT
# MAGIC       x.CustomerID,
# MAGIC       x.InvoiceDate,
# MAGIC       MIN(y.InvoiceDate) as NextInvoiceDate
# MAGIC     FROM
# MAGIC       CustomerPurchaseDates x
# MAGIC       INNER JOIN CustomerPurchaseDates y ON x.CustomerID = y.CustomerID
# MAGIC       AND x.InvoiceDate < y.InvoiceDate
# MAGIC     GROUP BY
# MAGIC       x.CustomerID,
# MAGIC       x.InvoiceDate
# MAGIC   ) a
# MAGIC GROUP BY
# MAGIC   CustomerID

# COMMAND ----------

# MAGIC %md ###Step 3: 顧客のメトリックを計算する
# MAGIC
# MAGIC 私たちが扱うデータセットは、生の取引履歴で構成されています。BTYDモデルを適用するためには、いくつかの顧客ごとの評価指標を導き出す必要があります。</p>
# MAGIC
# MAGIC * **Frequency** - 観測期間中の取引(買い物)回数。ただし、初回購入は除く。つまり、(個人の全取引回数 - 1)。同日に複数回取引があっても1回とカウントする。
# MAGIC * **Age (T)** - 初めての取引発生した日から現在までの経過日数。
# MAGIC * **Recency** - 初回の取引の日から直近(最後の)取引があった日までの経過日数。
# MAGIC
# MAGIC **Age (T)** などの指標を計算する際には、データセットがいつ終了するかを考慮する必要があることに注意してください。今日の日付を基準にしてこれらのメトリクスを計算すると、誤った結果になる可能性があります。そこで、データセットの最後の日付を特定し、それを *今日の日付* と定義して、すべての計算を行うことにします。
# MAGIC
# MAGIC ここでは、lifetimesライブラリに組み込まれた機能を使って、どのように計算を行うかをみていきます。

# COMMAND ----------

import lifetimes

# Pandas DataFrameにonline retailをロードします
orders_pd = spark.table("online_retail").toPandas()

# このヒストリカル・データセットの終了点として、最終取引日を設定する。
current_date = orders_pd["InvoiceDate"].max()

# 必要な顧客指標を計算する
metrics_pd = lifetimes.utils.summary_data_from_transaction_data(
    orders_pd,
    customer_id_col="CustomerID",
    datetime_col="InvoiceDate",
    observation_period_end=current_date,
    freq="D",
)

# 最初の数行を表示する
display(metrics_pd.head(10))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC lifetimesライブラリは、多くのPythonライブラリと同様に、シングルスレッドです。このライブラリを使用して大規模なトランザクションデータセットの顧客メトリクスを導出すると、システムを圧迫したり、単に完了までに時間がかかりすぎたりする可能性があります。そこで、Apache Sparkの分散機能を利用してこれらの指標を算出する方法を検討してみましょう。
# MAGIC
# MAGIC
# MAGIC 複雑なデータ操作にはSQLが使われることが多いので、まずはSparkのSQL文を使ってみます。
# MAGIC
# MAGIC このステートメントでは、まず各顧客の注文履歴を、
# MAGIC 1. 顧客ID
# MAGIC 1. 最初の購入日（first_at）
# MAGIC 1. 購入が確認された日（transaction_at）
# MAGIC 1. 現在の日付（この値にはデータセットの最後の日付を使用）
# MAGIC
# MAGIC で構成します。
# MAGIC
# MAGIC この履歴から、顧客ごとに、
# MAGIC 1. 取引された日の数のカウント（frequency）
# MAGIC 1. 最後の取引日から最初の取引日までの日数（recency）
# MAGIC 1. 現在の日付から最初の取引までの日数（T）
# MAGIC
# MAGIC をカウントすることができます。

# COMMAND ----------

# 顧客統計の要約を得るためのSQLステートメント
sql = """
  SELECT
    a.customerid as CustomerID,
    CAST(COUNT(DISTINCT a.transaction_at) - 1 as float) as frequency,
    CAST(DATEDIFF(MAX(a.transaction_at), a.first_at) as float) as recency,
    CAST(DATEDIFF(a.current_dt, a.first_at) as float) as T
  FROM ( -- customer order history
    SELECT DISTINCT
      x.customerid,
      z.first_at,
      TO_DATE(InvoiceDate, "MM/dd/yy HH:mm") as transaction_at,
      y.current_dt
    FROM online_retail x
    CROSS JOIN (SELECT MAX(TO_DATE(InvoiceDate, "MM/dd/yy HH:mm")) as current_dt FROM online_retail) y                                -- current date (according to dataset)
    INNER JOIN (SELECT customerid, MIN(TO_DATE(InvoiceDate, "MM/dd/yy HH:mm")) as first_at FROM online_retail GROUP BY customerid) z  -- first order per customer
      ON x.customerid=z.customerid
    WHERE x.customerid IS NOT NULL
    ) a
  GROUP BY a.customerid, a.current_dt, a.first_at
  ORDER BY CustomerID
  """

# Spark SQLとして実行します
metrics_sql = spark.sql(sql)

# display stats
display(metrics_sql)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC もちろん，Spark SQLでは，DataFrameをSQL文のみでアクセスする必要はありません。
# MAGIC
# MAGIC データサイエンティストの好みに合わせて、PySpark APIを使って同じ結果を導き出すこともできます。次のセルのコードは、比較のために、前のSQL文の構造を反映するように意図的に組み立てられています。

# COMMAND ----------

from pyspark.sql.functions import (
    to_date,
    datediff,
    max,
    min,
    countDistinct,
    count,
    sum,
    when,
)
from pyspark.sql.types import *

# 一時ビューにテーブルをロードする
online_retail = spark.table("online_retail")

# 有効な顧客注文を抽出します
x = online_retail.where(online_retail.CustomerID.isNotNull())

# データセットの最終日付を計算する
y = online_retail.groupBy().agg(
    max(to_date(online_retail.InvoiceDate, "MM/dd/yy HH:mm")).alias("current_dt")
)

# 顧客別に最初の取引日を計算
z = online_retail.groupBy(online_retail.CustomerID).agg(
    min(to_date(online_retail.InvoiceDate, "MM/dd/yy HH:mm")).alias("first_at")
)

# 顧客履歴と日付情報を組み合わせる
a = (
    x.crossJoin(y)
    .join(z, x.CustomerID == z.CustomerID, how="inner")
    .select(
        x.CustomerID.alias("customerid"),
        z.first_at,
        to_date(x.InvoiceDate, "MM/dd/yy HH:mm").alias("transaction_at"),
        y.current_dt,
    )
    .distinct()
)

# 顧客別の関連指標を計算する
metrics_api = (
    a.groupBy(a.customerid, a.current_dt, a.first_at)
    .agg(
        (countDistinct(a.transaction_at) - 1).cast(FloatType()).alias("frequency"),
        datediff(max(a.transaction_at), a.first_at).cast(FloatType()).alias("recency"),
        datediff(a.current_dt, a.first_at).cast(FloatType()).alias("T"),
    )
    .select("customerid", "frequency", "recency", "T")
    .orderBy("customerid")
)

display(metrics_api)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ここまでに算出したメトリクスは、全時系列データのサマリーを表しています。
# MAGIC
# MAGIC 次に、モデルの学習およびオーバーフィッティング回避の戦略を考える必要があります。時系列データのパターン認識の一般的なパターンは、時系列の初期部分（キャリブレーション期間と呼ばれる）でモデルをトレーニングし、時系列の後期部分（ホールドアウト期間と呼ばれる）で検証することです。こうすることで、未来の情報が学習期間に含まれることを防げます。
# MAGIC
# MAGIC lifetimesライブラリでは、キャリブレーション期間とホールドアウト期間を用いた顧客ごとのメトリクスの導出がシンプルな関数呼び出しで可能です。
# MAGIC
# MAGIC 今回のデータセットは限られた範囲のデータで構成されているため、ホールドアウト期間として過去90日分のデータを使用するよう、ライブラリの関数に与えています。この設定の構成を簡単に変更できるように、Databricksではウィジェットと呼ばれるシンプルなパラメータUIが実装されています。
# MAGIC
# MAGIC 注：次のセルを実行するとノートブックの最上部に、テキストボックス・ウィジェットが追加されます。これを使って、保留期間の日数を変更することができます。

# COMMAND ----------

# NotebookのWidgetを作成 (デフォルト: 90-days)
dbutils.widgets.text("holdout days", "90")

# COMMAND ----------

# DBTITLE 1,lifetimesライブラリを使って算出
from datetime import timedelta
import pandas as pd
import lifetimes

# InvoiceDateをdatetime型に変換する。
orders_pd["InvoiceDate"] = pd.to_datetime(orders_pd["InvoiceDate"])

# このヒストリカル・データセットの終了点として、最終取引日を設定する。
current_date = orders_pd["InvoiceDate"].max()

# キャリブレーション期間の終了を定義する
holdout_days = int(dbutils.widgets.get("holdout days"))
calibration_end_date = current_date - timedelta(days=holdout_days) # 最終取引日から90日遡った日付

# 必要な顧客指標を計算する
metrics_cal_pd = lifetimes.utils.calibration_and_holdout_data(
    orders_pd,
    customer_id_col="CustomerID",
    datetime_col="InvoiceDate",
    observation_period_end=current_date,
    calibration_period_end=calibration_end_date,
    freq="D",
)

# 最初の数行を表示する
metrics_cal_pd.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ホールドアウト期間を表すduration_holdout以外にも、ホールドアウト期間中の取引回数を示すfrequency_holdoutが追加されています。 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC
# MAGIC 前回同様、Spark SQLを利用して同じ情報を得ることができます。ここでも、SQL文とSQL API(Pythonなどから呼び出す)の両方を使って検討します。
# MAGIC
# MAGIC このSQL文を理解するには、まず2つの主要部分に分かれていることを認識してください。
# MAGIC
# MAGIC 1つ目の部分では、前のクエリの例で行ったのと同様に、校正期間の顧客ごとにコアメトリクス、すなわち再帰性、頻度、年齢（T）を計算します。
# MAGIC
# MAGIC 2つ目の部分では、顧客ごとにホールドアウトした顧客の購入日の数を計算します。この値(frequency_holdout)は、キャリブレーション期間とホールドアウト期間の両方にわたる顧客の全取引履歴を調べたときに、校正期間の頻度(frequency_cal)に追加される増分を表しています。
# MAGIC
# MAGIC
# MAGIC ロジックを単純化するために、CustomerHistoryという名前の共通テーブル式（CTE）をクエリの先頭に定義しています。このクエリは、顧客の取引履歴を構成する関連する日付を抽出するもので、前回調べたSQL文の中心にあるロジックをよく反映しています。唯一の違いは、保留期間の日数(duration_holdout)を含めていることです。

# COMMAND ----------

# DBTITLE 1,SQL(Spark SQL)を使って算出
sql = """
WITH CustomerHistory 
  AS (
    SELECT  -- nesting req'ed b/c can't SELECT DISTINCT on widget parameter
      m.*,
      getArgument('holdout days') as duration_holdout
    FROM (
      SELECT DISTINCT
        x.customerid,
        z.first_at,
        TO_DATE(x.invoicedate, "MM/dd/yy HH:mm") as transaction_at,
        y.current_dt
      FROM online_retail x
      CROSS JOIN (SELECT MAX(TO_DATE(invoicedate, "MM/dd/yy HH:mm")) as current_dt FROM online_retail) y                                -- current date (according to dataset)
      INNER JOIN (SELECT customerid, MIN(TO_DATE(invoicedate, "MM/dd/yy HH:mm")) as first_at FROM online_retail GROUP BY customerid) z  -- first order per customer
        ON x.customerid=z.customerid
      WHERE x.customerid IS NOT NULL
    ) m
  )
SELECT
    a.customerid as CustomerID,
    a.frequency as frequency_cal,
    a.recency as recency_cal,
    a.T as T_cal,
    COALESCE(b.frequency_holdout, 0.0) as frequency_holdout,
    a.duration_holdout
FROM ( -- CALIBRATION PERIOD CALCULATIONS
    SELECT
        p.customerid,
        CAST(p.duration_holdout as float) as duration_holdout,
        CAST(DATEDIFF(MAX(p.transaction_at), p.first_at) as float) as recency,
        CAST(COUNT(DISTINCT p.transaction_at) - 1 as float) as frequency,
        CAST(DATEDIFF(DATE_SUB(p.current_dt, int(p.duration_holdout)), p.first_at) as float) as T
    FROM CustomerHistory p
    WHERE p.transaction_at < DATE_SUB(p.current_dt, int(p.duration_holdout))  -- LIMIT THIS QUERY TO DATA IN THE CALIBRATION PERIOD
    GROUP BY p.customerid, p.first_at, p.current_dt, p.duration_holdout
  ) a
LEFT OUTER JOIN ( -- HOLDOUT PERIOD CALCULATIONS
  SELECT
    p.customerid,
    CAST(COUNT(DISTINCT p.transaction_at) as float) as frequency_holdout
  FROM CustomerHistory p
  WHERE 
    p.transaction_at >= DATE_SUB(p.current_dt, int(p.duration_holdout) ) AND  -- LIMIT THIS QUERY TO DATA IN THE HOLDOUT PERIOD
    p.transaction_at <= p.current_dt
  GROUP BY p.customerid
  ) b
  ON a.customerid=b.customerid
ORDER BY CustomerID
"""

metrics_cal_sql = spark.sql(sql)
display(metrics_cal_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC そして、これに相当するPySpark APIのロジックは以下の通りです。

# COMMAND ----------

from pyspark.sql.functions import avg, date_sub, coalesce, lit, expr

# valid customer orders
x = online_retail.where(online_retail.CustomerID.isNotNull())

# calculate last date in dataset
y = online_retail.groupBy().agg(
    max(to_date(online_retail.InvoiceDate, "MM/dd/yy HH:mm")).alias("current_dt")
)

# calculate first transaction date by customer
z = online_retail.groupBy(online_retail.CustomerID).agg(
    min(to_date(online_retail.InvoiceDate, "MM/dd/yy HH:mm")).alias("first_at")
)

# combine customer history with date info (CUSTOMER HISTORY)
p = (
    x.crossJoin(y)
    .join(z, x.CustomerID == z.CustomerID, how="inner")
    .withColumn("duration_holdout", lit(int(dbutils.widgets.get("holdout days"))))
    .select(
        x.CustomerID.alias("customerid"),
        z.first_at,
        to_date(x.InvoiceDate, "MM/dd/yy HH:mm").alias("transaction_at"),
        y.current_dt,
        "duration_holdout",
    )
    .distinct()
)

# calculate relevant metrics by customer
# note: date_sub requires a single integer value unless employed within an expr() call
a = (
    p.where(p.transaction_at < expr("date_sub(current_dt, duration_holdout)"))
    .groupBy(p.customerid, p.current_dt, p.duration_holdout, p.first_at)
    .agg(
        (countDistinct(p.transaction_at) - 1).cast(FloatType()).alias("frequency_cal"),
        datediff(max(p.transaction_at), p.first_at)
        .cast(FloatType())
        .alias("recency_cal"),
        datediff(expr("date_sub(current_dt, duration_holdout)"), p.first_at)
        .cast(FloatType())
        .alias("T_cal"),
    )
)

b = (
    p.where(
        (p.transaction_at >= expr("date_sub(current_dt, duration_holdout)"))
        & (p.transaction_at <= p.current_dt)
    )
    .groupBy(p.customerid)
    .agg(countDistinct(p.transaction_at).cast(FloatType()).alias("frequency_holdout"))
)

metrics_cal_api = (
    a.join(b, a.customerid == b.customerid, how="left")
    .select(
        a.customerid.alias("CustomerID"),
        a.frequency_cal,
        a.recency_cal,
        a.T_cal,
        coalesce(b.frequency_holdout, lit(0.0)).alias("frequency_holdout"),
        a.duration_holdout,
    )
    .orderBy("CustomerID")
)

display(metrics_cal_api)

# COMMAND ----------

# MAGIC %md 
# MAGIC サマリー統計を使って、これらの異なるロジックのユニットが同じ結果を返していることを再度確認することができます。

# COMMAND ----------

# DBTITLE 1,lifetimesライブラリを使って算出 - 結果確認
# summary data from lifetimes
metrics_cal_pd.describe()

# COMMAND ----------

# DBTITLE 1,SQL(Spark SQL)を使って算出 - 結果確認
# summary data from SQL statement
metrics_cal_sql.toPandas().describe()

# COMMAND ----------

# DBTITLE 1,Python(pyspark)を使って算出 - 結果確認
# summary data from pyspark.sql API
metrics_cal_api.toPandas().describe()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC データの準備はほぼ完了しています。最後に、リピート購入のない顧客を除外します。すなわち、frequencyまたはfrequency_calが0の場合です。
# MAGIC
# MAGIC これは使用するPareto/NBDおよびBG/NBDモデルが、リピート取引のある顧客に対してのみ計算を行うことに焦点を当てているためです。BG/NBDモデルを修正したMBG/NBDモデルは、リピート購入のない顧客を対象としており、lifetimesライブラリでサポートされています。
# MAGIC
# MAGIC
# MAGIC 注: ノートブックのこのセクションで以前に行ったサイド・バイ・サイドの比較との一貫性を保つために、pandasとSpark DataFramesの両方にフィルターをかける方法を示しています。 実際の実装では、データの準備にpandasとSpark DataFramesのどちらを使用するかを選択するだけです。
# MAGIC

# COMMAND ----------

# リピートなしの顧客を除外(フルデータセットの方のDataFrame)し、テーブルとして保存
filtered = metrics_api.where(metrics_api.frequency > 0)
(
  filtered.write
  .mode("overwrite")
  .saveAsTable("user_metrics_all")
  )

## リピートなしの顧客を除外(キャリブレーションのDataFrame)し、テーブルとして保存
filtered_cal = metrics_cal_api.where(metrics_cal_api.frequency_cal > 0)
(
  filtered_cal.write
  .mode("overwrite")
  .saveAsTable("user_metrics_training")
  )
