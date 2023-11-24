-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ## Challenge2. 優良顧客の特定(11:50-12:00)
-- MAGIC ### 本ノートブックの目的：顧客生存確率を元に、アプローチするユーザーを特定する
-- MAGIC
-- MAGIC その過程でDatabricksに関する以下の事項を学習します
-- MAGIC - カタログに保存したテーブルのGUIによるダッシュボード化
-- MAGIC - ダッシュボードへの計算リソース(Serverless ウェアハウス)のアタッチ
-- MAGIC - ダッシュボードで抽出されたデータのダウンロード
-- MAGIC
-- MAGIC なお、詳細な手順は[こちらの動画](https://youtu.be/QgxGXqzZ0JA)で解説しています