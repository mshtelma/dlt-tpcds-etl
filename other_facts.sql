-- Databricks notebook source
-- MAGIC 
-- MAGIC %md # Item

-- COMMAND ----------

CREATE LIVE VIEW item_landing
COMMENT "Item Landing"
AS SELECT *
FROM ${src_db}.item;

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
FROM ${src_db}.customer_address;

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

-- MAGIC %md # Customer

-- COMMAND ----------

CREATE LIVE VIEW customer_landing
COMMENT "Customer Landing"
AS SELECT *
FROM ${src_db}.customer;

-- COMMAND ----------

CREATE LIVE TABLE customer_staging
COMMENT "Customer Staging"
AS SELECT *
FROM live.customer_landing;

-- COMMAND ----------

CREATE LIVE TABLE customer_curated
COMMENT "Customer Curated"
AS SELECT *
FROM live.customer_staging;

-- COMMAND ----------

-- MAGIC %md # Promotion

-- COMMAND ----------

CREATE LIVE VIEW promotion_landing
COMMENT "Promotion Landing"
AS SELECT *
FROM ${src_db}.promotion;

-- COMMAND ----------

CREATE LIVE TABLE promotion_staging
COMMENT "Promotion Staging"
AS SELECT *
FROM live.promotion_landing;

-- COMMAND ----------

CREATE LIVE TABLE promotion_curated
COMMENT "Promotion Curated"
AS SELECT *
FROM live.promotion_staging;

-- COMMAND ----------

-- MAGIC %md # Store

-- COMMAND ----------

CREATE LIVE VIEW store_landing
COMMENT "Store Landing"
AS SELECT *
FROM ${src_db}.store;

-- COMMAND ----------

CREATE LIVE TABLE store_staging
COMMENT "Store Staging"
AS SELECT *
FROM live.store_landing;

-- COMMAND ----------

CREATE LIVE TABLE store_curated
COMMENT "Store Curated"
AS SELECT *
FROM live.store_staging;
