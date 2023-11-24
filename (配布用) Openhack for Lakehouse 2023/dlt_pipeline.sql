-- Databricks notebook source
-- DBTITLE 1,online retailの生データをDelta化する
CREATE
OR REFRESH STREAMING LIVE TABLE dlt_online_retail_raw COMMENT "生のorderデータをDelta Lake化する" TBLPROPERTIES ("quality" = "bronze") AS
SELECT
  current_timestamp() processing_time,
  *
FROM
  cloud_files(
    "dbfs:/Volumes/main/db_hackathon4lakehouse_your_name/volume_your_name",
    "csv",
    map(
      "cloudFiles.inferColumnTypes",
      "true",
      "overwriteSchema",
      "true",
      "cloudFiles.schemaHints",
      "`InvoiceNo` STRING, 
    `StockCode` STRING, 
    `Description` STRING, 
    `Quantity` INT, 
    `InvoiceDate` STRING, 
    `UnitPrice` DOUBLE, 
    `CustomerID` INT, 
    `Country` STRING "
    )
  )

-- COMMAND ----------

-- DBTITLE 1,不要列を削除する
SET
  spark.sql.legacy.timeParserPolicy = LEGACY;
CREATE
OR REFRESH LIVE TABLE dlt_online_retail TBLPROPERTIES ("quality" = "bronze") AS
SELECT
  *,
  TO_DATE(InvoiceDate, "MM/dd/yy HH:mm") as InvoiceDateFmt
FROM
  LIVE.dlt_online_retail_raw

-- COMMAND ----------

-- DBTITLE 1,ホールドアウト期間も含めたユーザーメトリクス
CREATE
  OR REFRESH LIVE TABLE dlt_user_metrics_all (
    CONSTRAINT valid_freq EXPECT (frequency > 0) ON VIOLATION DROP ROW
  ) COMMENT "全件から集計を行ったメトリクス" TBLPROPERTIES ("quality" = "silver") AS
SELECT
  a.customerid as CustomerID,
  CAST(COUNT(DISTINCT a.transaction_at) - 1 as float) as frequency,
  CAST(
    DATEDIFF(MAX(a.transaction_at), a.first_at) as float
  ) as recency,
  CAST(DATEDIFF(a.current_dt, a.first_at) as float) as T
FROM
  (
    -- 注文履歴
    SELECT
      DISTINCT x.customerid,
      z.first_at,
      InvoiceDateFmt as transaction_at,
      y.current_dt
    FROM
      LIVE.dlt_online_retail x
      CROSS JOIN (
        SELECT
          MAX(InvoiceDateFmt) as current_dt
        FROM
          LIVE.dlt_online_retail
      ) y -- データセットの最終日
      INNER JOIN (
        SELECT
          customerid,
          MIN(InvoiceDateFmt) as first_at
        FROM
          LIVE.dlt_online_retail
        GROUP BY
          customerid
      ) z -- お客さんごとの最初の注文日
      ON x.customerid = z.customerid
    WHERE
      x.customerid IS NOT NULL
  ) a
GROUP BY
  a.customerid,
  a.current_dt,
  a.first_at
ORDER BY
  CustomerID

-- COMMAND ----------

-- DBTITLE 1,キャリブレーション期間のデータ
CREATE
OR REFRESH LIVE TABLE dlt_user_metrics_training COMMENT "キャリブレーション期間" TBLPROPERTIES ("quality" = "silver") AS WITH CustomerHistory AS (
  SELECT
    -- ウィジェット・パラメータでSELECT DISTINCTできないため、ネスティングが必要です
    m.*,
    "${holdout_days}" as duration_holdout
  FROM
    (
      SELECT
        DISTINCT x.customerid,
        z.first_at,
        InvoiceDateFmt as transaction_at,
        y.current_dt
      FROM
        LIVE.dlt_online_retail x
        CROSS JOIN (
          SELECT
            MAX(InvoiceDateFmt) as current_dt
          FROM
            LIVE.dlt_online_retail
        ) y -- データセットの最終日
        INNER JOIN (
          SELECT
            customerid,
            MIN(InvoiceDateFmt) as first_at
          FROM
            LIVE.dlt_online_retail
          GROUP BY
            customerid
        ) z -- お客さんごとの最初の注文日
        ON x.customerid = z.customerid
      WHERE
        x.customerid IS NOT NULL
    ) m
)
SELECT
  a.customerid as CustomerID,
  a.frequency as frequency_cal,
  a.recency as recency_cal,
  a.T as T_cal,
  COALESCE(b.frequency_holdout, 0.0) as frequency_holdout,
  a.duration_holdout
FROM
  (
    -- キャリブレーション期間の計算
    SELECT
      p.customerid,
      CAST(p.duration_holdout as float) as duration_holdout,
      CAST(
        DATEDIFF(MAX(p.transaction_at), p.first_at) as float
      ) as recency,
      CAST(COUNT(DISTINCT p.transaction_at) - 1 as float) as frequency,
      CAST(
        DATEDIFF(
          DATE_SUB(p.current_dt, int(p.duration_holdout)),
          p.first_at
        ) as float
      ) as T
    FROM
      CustomerHistory p
    WHERE
      p.transaction_at < DATE_SUB(p.current_dt, int(p.duration_holdout)) -- このクエリーをキャリブレーション期間のデータに限定する。
    GROUP BY
      p.customerid,
      p.first_at,
      p.current_dt,
      p.duration_holdout
  ) a
  LEFT OUTER JOIN (
    -- ホールドアウト期間の計算
    SELECT
      p.customerid,
      CAST(COUNT(DISTINCT p.transaction_at) as float) as frequency_holdout
    FROM
      CustomerHistory p
    WHERE
      p.transaction_at >= DATE_SUB(p.current_dt, int(p.duration_holdout))
      AND -- このクエリーをホールドアウト期間のデータに限定する
      p.transaction_at <= p.current_dt
    GROUP BY
      p.customerid
  ) b ON a.customerid = b.customerid
ORDER BY
  CustomerID
