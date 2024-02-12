# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()
# COMMAND ----------

import json
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineSpec


def read_and_prepare_pipeline_config(file_name: str, prefix: str) -> PipelineSpec:
    with open(file_name, "r") as f:
        pipeline_spec = PipelineSpec.from_dict(json.load(f))
        for lib in pipeline_spec.libraries:
            lib.notebook.path = str(Path(prefix).joinpath(lib.notebook.path))
        return pipeline_spec


def create_or_update_pipeline(w: WorkspaceClient, pipeline_spec: PipelineSpec):
    existing_pipelines = list(
        w.pipelines.list_pipelines(filter=f"name like '{pipeline_spec.name}'")
    )
    if len(existing_pipelines) > 0:
        print("Existing pipeline was found. Updating it...")
        existing_pipeline = existing_pipelines[0]
        w.pipelines.update(
            pipeline_id=existing_pipeline.pipeline_id, **pipeline_spec.__dict__
        )
    else:
        print("Creating a new pipeline...")
        resp = w.pipelines.create(**pipeline_spec.__dict__)
        print(f"Created a new pipeline with ID {resp.pipeline_id}.")


w = WorkspaceClient()
prefix = "/Repos/michael.shtelma@databricks.com/dlt-tpcds-etl/"
pipeline_spec = read_and_prepare_pipeline_config("tpcds_pipeline_config.json", prefix)
create_or_update_pipeline(w, pipeline_spec)

pipeline_spec = read_and_prepare_pipeline_config(
    "python_test_pipeline_config.json", prefix
)
create_or_update_pipeline(w, pipeline_spec)
