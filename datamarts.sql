-- Databricks notebook source

-- MAGIC %md # Data Marts

-- COMMAND ----------

CREATE LIVE TABLE data_mart_60 AS with cs as (
  select
    i_item_id,
    sum(cs_ext_sales_price) total_sales
  from
    live.catalog_sales_curated,
    live.date_dim_curated,
    live.customer_address_curated,
    live.item_curated
  where
    i_item_id in (
      select
        i_item_id
      from
        live.item_curated
      where
        i_category in ('Music')
    )
    and cs_item_sk = i_item_sk
    and cs_sold_date_sk = d_date_sk
    and d_year = 1998
    and d_moy = 9
    and cs_bill_addr_sk = ca_address_sk
    and ca_gmt_offset = -5
  group by
    i_item_id
),
ws as (
  select
    i_item_id,
    sum(ws_ext_sales_price) total_sales
  from
    live.web_sales_curated,
    live.date_dim_curated,
    live.customer_address_curated,
    live.item_curated
  where
    i_item_id in (
      select
        i_item_id
      from
        live.item_curated
      where
        i_category in ('Music')
    )
    and ws_item_sk = i_item_sk
    and ws_sold_date_sk = d_date_sk
    and d_year = 1998
    and d_moy = 9
    and ws_bill_addr_sk = ca_address_sk
    and ca_gmt_offset = -5
  group by
    i_item_id
)
select
  i_item_id,
  sum(total_sales) total_sales
from
  (
    select
      *
    from
      cs
    union all
    select
      *
    from
      ws
  ) tmp1
group by
  i_item_id
order by
  i_item_id,
  total_sales -- limit 100
