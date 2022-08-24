-- Databricks notebook source
-- MAGIC %md # Web Sales

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE web_sales_curated (
  ws_sold_date_sk INT,
  ws_sold_time_sk INT,
  ws_ship_date_sk INT,
  ws_item_sk INT,
  ws_bill_customer_sk INT,
  ws_bill_cdemo_sk INT,
  ws_bill_hdemo_sk INT,
  ws_bill_addr_sk INT,
  ws_ship_customer_sk INT,
  ws_ship_cdemo_sk INT,
  ws_ship_hdemo_sk INT,
  ws_ship_addr_sk INT,
  ws_web_page_sk INT,
  ws_web_site_sk INT,
  ws_ship_mode_sk INT,
  ws_warehouse_sk INT,
  ws_promo_sk INT,
  ws_order_number BIGINT,
  ws_quantity INT,
  ws_wholesale_cost DECIMAL(7, 2),
  ws_list_price DECIMAL(7, 2),
  ws_sales_price DECIMAL(7, 2),
  ws_ext_discount_amt DECIMAL(7, 2),
  ws_ext_sales_price DECIMAL(7, 2),
  ws_ext_wholesale_cost DECIMAL(7, 2),
  ws_ext_list_price DECIMAL(7, 2),
  ws_ext_tax DECIMAL(7, 2),
  ws_coupon_amt DECIMAL(7, 2),
  ws_ext_ship_cost DECIMAL(7, 2),
  ws_net_paid DECIMAL(7, 2),
  ws_net_paid_inc_tax DECIMAL(7, 2),
  ws_net_paid_inc_ship DECIMAL(7, 2),
  ws_net_paid_inc_ship_tax DECIMAL(7, 2),
  ws_net_profit DECIMAL(7, 2)
) USING delta
PARTITIONED BY (ws_sold_date_sk)


-- COMMAND ----------
CREATE STREAMING LIVE VIEW web_sales_landing (

)
COMMENT "Web Sales Landing"
AS SELECT *
FROM STREAM(tpcds1gb.web_sales);
-- COMMAND ----------

CREATE STREAMING LIVE TABLE web_sales_staging (
)
COMMENT "Web Sales Staging"
AS SELECT *
FROM STREAM(live.web_sales_landing);

-- COMMAND ----------

CREATE STREAMING LIVE VIEW web_sales_clean AS
SELECT
  ws.*
FROM
  STREAM(live.web_sales_staging) ws
  join live.date_dim_curated dd1 on ws.ws_sold_date_sk = dd1.d_date_sk
  join live.date_dim_curated dd2 on ws.ws_ship_date_sk = dd2.d_date_sk

-- COMMAND ----------

APPLY CHANGES INTO live.web_sales_curated
FROM
  stream(live.web_sales_clean) KEYS (ws_item_sk, ws_order_number) SEQUENCE BY ws_sold_date_sk

-- COMMAND ----------

CREATE LIVE TABLE web_sales_curated_pk_constraint(
  CONSTRAINT unique_pk EXPECT (num_entries = 1)
)
AS SELECT ws_item_sk, ws_order_number, count(*) as num_entries
FROM LIVE.web_sales_curated
GROUP BY ws_item_sk, ws_order_number


-- COMMAND ----------

CREATE STREAMING LIVE TABLE web_sales_quarantine AS
SELECT
  ws.*
FROM
  STREAM(live.web_sales_staging) ws
  LEFT ANTI JOIN live.date_dim_curated dd1 on ws.ws_sold_date_sk = dd1.d_date_sk
UNION ALL
SELECT
  ws.*
FROM
  STREAM(live.web_sales_staging) ws
  LEFT ANTI JOIN live.date_dim_curated dd2 on ws.ws_ship_date_sk = dd2.d_date_sk

-- COMMAND ----------