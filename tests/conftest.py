#!/usr/bin/env python3.7
#
# conftest - Configuration file for our unit tests
#
# Copyright (C) 2020 by Alonso Roman <alonso.roman@outlook.com>
#
#
import pytest
from pyspark.sql import SparkSession
import shutil
from tempfile import mkdtemp

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Function to create an SparkSession for our unit tests,
    for those which needed one
    :return: Spark Session
    """
    spark = (
        SparkSession.builder
                    .master("local[2]")
                    .appName("green_flag_test")
                    .config("spark.executorEnv.PYTHONHASHSEED", "0")
                    .getOrCreate()
    )
    return spark

@pytest.fixture(scope="function")
def tmp_dir():
    """
    Create a temporary directory and return the path
    After test has been executed it will remove it
    :return: Temporary directory
    """
    output_dir = mkdtemp()
    yield output_dir
    shutil.rmtree(output_dir)