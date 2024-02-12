# Databricks notebook source
# MAGIC %pip install -e ../..
# MAGIC dbutils.library.restartPython()
# COMMAND ----------

import dlt
from pyspark.sql.functions import *

from etllogic import transform_logic


json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"


@dlt.table(
    comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)
def clickstream_raw():
    return transform_logic(spark.read.format("json").load(json_path))
