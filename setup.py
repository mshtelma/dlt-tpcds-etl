import os

from setuptools import setup, find_packages

setup(
    name="dlt_tpc_ds_demo",
    version="0.0.1",
    description="ETL for TPC-DS dataset using Databricks Delta Live Tables",
    author="Michael Shtelma",
    url="",
    install_requires=["databricks-sdk==0.19.0"],
    packages=find_packages(),
    long_description="",
    long_description_content_type="text/markdown",
)
