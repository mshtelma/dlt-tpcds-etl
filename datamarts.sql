-- Databricks notebook source
-- MAGIC %md # Data Marts

-- COMMAND ----------

CREATE LIVE TABLE data_mart_60 AS with ss as (
  select
    i_item_id,
    sum(ss_ext_sales_price) total_sales
  from
    live.store_sales_curated,
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
    and ss_item_sk = i_item_sk
    and ss_sold_date_sk = d_date_sk
    and d_year = 1998
    and d_moy = 9
    and ss_addr_sk = ca_address_sk
    and ca_gmt_offset = -5
  group by
    i_item_id
),
cs as (
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
      ss
    union all
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
  total_sales

-- COMMAND ----------

-- MAGIC %md # Data Mart 61

-- COMMAND ----------

CREATE LIVE TABLE data_mart_61 AS
select
  promotions,
  total,
  cast(promotions as decimal(15, 4)) / cast(total as decimal(15, 4)) * 100 result
from
  (
    select
      sum(ss_ext_sales_price) promotions
    from
      live.store_sales_curated,
      live.store_curated,
      live.promotion_curated,
      live.date_dim_curated,
      live.customer_curated,
      live.customer_address_curated,
      live.item_curated
    where
      ss_sold_date_sk = d_date_sk
      and ss_store_sk = s_store_sk
      and ss_promo_sk = p_promo_sk
      and ss_customer_sk = c_customer_sk
      and ca_address_sk = c_current_addr_sk
      and ss_item_sk = i_item_sk
      and ca_gmt_offset = -5
      and i_category = 'Jewelry'
      and (
        p_channel_dmail = 'Y'
        or p_channel_email = 'Y'
        or p_channel_tv = 'Y'
      )
      and s_gmt_offset = -5
      and d_year = 1998
      and d_moy = 11
  ) promotional_sales
  cross join (
    select
      sum(ss_ext_sales_price) total
    from
      live.store_sales_curated,
      live.store_curated,
      live.date_dim_curated,
      live.customer_curated,
      live.customer_address_curated,
      live.item_curated
    where
      ss_sold_date_sk = d_date_sk
      and ss_store_sk = s_store_sk
      and ss_customer_sk = c_customer_sk
      and ca_address_sk = c_current_addr_sk
      and ss_item_sk = i_item_sk
      and ca_gmt_offset = -5
      and i_category = 'Jewelry'
      and s_gmt_offset = -5
      and d_year = 1998
      and d_moy = 11
  ) all_sales
order by
  promotions,
  total
