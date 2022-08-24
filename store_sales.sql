-- Databricks notebook source
-- MAGIC %md # Web Sales

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE store_sales_curated (
  ss_sold_date_sk INT,
  ss_sold_time_sk INT,
  ss_item_sk INT,
  ss_customer_sk INT,
  ss_cdemo_sk INT,
  ss_hdemo_sk INT,
  ss_addr_sk INT,
  ss_store_sk INT,
  ss_promo_sk INT,
  ss_ticket_number BIGINT,
  ss_quantity INT,
  ss_wholesale_cost DECIMAL(7, 2),
  ss_list_price DECIMAL(7, 2),
  ss_sales_price DECIMAL(7, 2),
  ss_ext_discount_amt DECIMAL(7, 2),
  ss_ext_sales_price DECIMAL(7, 2),
  ss_ext_wholesale_cost DECIMAL(7, 2),
  ss_ext_list_price DECIMAL(7, 2),
  ss_ext_tax DECIMAL(7, 2),
  ss_coupon_amt DECIMAL(7, 2),
  ss_net_paid DECIMAL(7, 2),
  ss_net_paid_inc_tax DECIMAL(7, 2),
  ss_net_profit DECIMAL(7, 2)
) USING delta PARTITIONED BY (ss_sold_date_sk);

-- COMMAND ----------

CREATE STREAMING LIVE VIEW store_sales_landing () COMMENT "Store Sales Landing" AS
SELECT
  *
FROM
  STREAM(tpcds1gb.store_sales);

-- COMMAND ----------

CREATE STREAMING LIVE TABLE store_sales_staging () COMMENT "Store Sales Staging" AS
SELECT
  *
FROM
  STREAM(live.store_sales_landing);

-- COMMAND ----------

CREATE STREAMING LIVE VIEW store_sales_clean AS
SELECT
  ss.*
FROM
  STREAM(live.store_sales_staging) ss
  join live.date_dim_curated dd1 on ss.ss_sold_date_sk = dd1.d_date_sk


-- COMMAND ----------

APPLY CHANGES INTO live.store_sales_curated
FROM
  stream(live.store_sales_clean) KEYS (ss_item_sk, ss_ticket_number) SEQUENCE BY ss_sold_date_sk

-- COMMAND ----------

CREATE LIVE TABLE store_sales_curated_pk_constraint(
  CONSTRAINT unique_pk EXPECT (num_entries = 1)
)
AS SELECT ss_item_sk, ss_ticket_number, count(*) as num_entries
FROM LIVE.store_sales_curated
GROUP BY ss_item_sk, ss_ticket_number


-- COMMAND ----------

CREATE STREAMING LIVE TABLE store_sales_quarantine AS
SELECT
  ss.*
FROM
  STREAM(live.store_sales_staging) ss
  LEFT ANTI JOIN live.date_dim_curated dd1 on ss.ss_sold_date_sk = dd1.d_date_sk

