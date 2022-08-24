-- Databricks notebook source


-- MAGIC %md # Item

-- COMMAND ----------

CREATE LIVE VIEW item_landing
COMMENT "Item Landing"
AS SELECT *
FROM tpcds1gb.item;

-- COMMAND ----------

CREATE LIVE TABLE item_staging
COMMENT "Item Staging"
AS SELECT *
FROM live.item_landing;

-- COMMAND ----------

CREATE LIVE TABLE item_curated
COMMENT "Item Curated"
AS SELECT *
FROM live.item_staging;

-- COMMAND ----------

-- MAGIC %md # Customer Address

-- COMMAND ----------

CREATE LIVE VIEW customer_address_landing
COMMENT "Customer Address Landing"
AS SELECT *
FROM tpcds1gb.customer_address;

-- COMMAND ----------

CREATE LIVE TABLE customer_address_staging
COMMENT "Customer Address Staging"
AS SELECT *
FROM live.customer_address_landing;

-- COMMAND ----------

CREATE LIVE TABLE customer_address_curated
COMMENT "Customer Address Curated"
AS SELECT *
FROM live.customer_address_staging;

-- COMMAND ----------
