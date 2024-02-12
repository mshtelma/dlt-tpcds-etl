-- Databricks notebook source
select * from delta.`dbfs:/Users/msh/dlt/msh_dlt_tpcds1tb/system/events`

-- COMMAND ----------

SELECT
  id,
  timestamp,
  details :flow_progress.metrics.num_output_rows as output_records,
  details :flow_progress.data_quality.dropped_records,
  details :flow_progress.status as status_update,
  explode(
    from_json(
      details :flow_progress.data_quality.expectations,
      'array<struct<dataset: string, failed_records: bigint, name: string, passed_records: bigint>>'
    )
  ) expectations
FROM
  delta.`dbfs:/Users/msh/dlt/msh_dlt_tpcds1tb/system/events`
where
  details :flow_progress.status = 'COMPLETED'
  and details :flow_progress.data_quality.expectations is not null
ORDER BY
  timestamp

-- COMMAND ----------


