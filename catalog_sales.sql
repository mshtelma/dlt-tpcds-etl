-- Databricks notebook source


-- MAGIC %md # Catalog Sales Table

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE catalog_sales_curated (
  cs_sold_date_sk INT,
  cs_sold_time_sk INT,
  cs_ship_date_sk INT,
  cs_bill_customer_sk INT,
  cs_bill_cdemo_sk INT,
  cs_bill_hdemo_sk INT,
  cs_bill_addr_sk INT,
  cs_ship_customer_sk INT,
  cs_ship_cdemo_sk INT,
  cs_ship_hdemo_sk INT,
  cs_ship_addr_sk INT,
  cs_call_center_sk INT,
  cs_catalog_page_sk INT,
  cs_ship_mode_sk INT,
  cs_warehouse_sk INT,
  cs_item_sk INT,
  cs_promo_sk INT,
  cs_order_number BIGINT,
  cs_quantity INT,
  cs_wholesale_cost DECIMAL(7, 2),
  cs_list_price DECIMAL(7, 2),
  cs_sales_price DECIMAL(7, 2),
  cs_ext_discount_amt DECIMAL(7, 2),
  cs_ext_sales_price DECIMAL(7, 2),
  cs_ext_wholesale_cost DECIMAL(7, 2),
  cs_ext_list_price DECIMAL(7, 2),
  cs_ext_tax DECIMAL(7, 2),
  cs_coupon_amt DECIMAL(7, 2),
  cs_ext_ship_cost DECIMAL(7, 2),
  cs_net_paid DECIMAL(7, 2),
  cs_net_paid_inc_tax DECIMAL(7, 2),
  cs_net_paid_inc_ship DECIMAL(7, 2),
  cs_net_paid_inc_ship_tax DECIMAL(7, 2),
  cs_net_profit DECIMAL(7, 2)
) USING delta
PARTITIONED BY (cs_sold_date_sk)

-- COMMAND ----------
CREATE STREAMING LIVE VIEW catalog_sales_landing (

)
COMMENT "Catalog Sales Landing"
AS SELECT *
FROM STREAM(tpcds1gb.catalog_sales);
-- COMMAND ----------

CREATE STREAMING LIVE TABLE catalog_sales_staging (

)
COMMENT "Cleansed cdc data, tracking data quality with a view. We ensude valid JSON, id and operation type"
AS SELECT *
FROM STREAM(live.catalog_sales_landing);

-- COMMAND ----------

CREATE STREAMING LIVE VIEW catalog_sales_clean AS
SELECT
  cs.*
FROM
  STREAM(live.catalog_sales_staging) cs
  join live.date_dim_curated dd1 on cs.cs_sold_date_sk = dd1.d_date_sk
  join live.date_dim_curated dd2 on cs.cs_ship_date_sk = dd2.d_date_sk

-- COMMAND ----------

APPLY CHANGES INTO live.catalog_sales_curated
FROM
  stream(live.catalog_sales_clean) KEYS (cs_item_sk, cs_order_number) SEQUENCE BY cs_sold_date_sk

-- COMMAND ----------

CREATE LIVE TABLE catalog_sales_curated_pk_constraint(
  CONSTRAINT unique_pk EXPECT (num_entries = 1)
)
AS SELECT cs_item_sk, cs_order_number, count(*) as num_entries
FROM LIVE.catalog_sales_curated
GROUP BY cs_item_sk, cs_order_number


-- COMMAND ----------

CREATE STREAMING LIVE TABLE catalog_sales_quarantine AS
SELECT
  cs.*
FROM
  STREAM(live.catalog_sales_staging) cs
  LEFT ANTI JOIN live.date_dim_curated dd1 on cs.cs_sold_date_sk = dd1.d_date_sk
UNION ALL
SELECT
  cs.*
FROM
  STREAM(live.catalog_sales_staging) cs
  LEFT ANTI JOIN live.date_dim_curated dd2 on cs.cs_ship_date_sk = dd2.d_date_sk

-- COMMAND ----------
