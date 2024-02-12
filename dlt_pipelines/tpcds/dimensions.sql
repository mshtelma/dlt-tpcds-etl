-- Databricks notebook source

-- MAGIC %md # Date Dim

-- COMMAND ----------

CREATE LIVE VIEW date_dim_landing
COMMENT "Date Dim Landing"
AS SELECT *
FROM ${src_db}.date_dim;

-- COMMAND ----------

CREATE LIVE TABLE date_dim_staging
COMMENT "Date Dim Staging"
AS SELECT *
FROM live.date_dim_landing;

-- COMMAND ----------

CREATE
LIVE TABLE date_dim_curated
COMMENT "Date Dim Curated"
AS
SELECT *
FROM live.date_dim_staging;

-- COMMAND ----------