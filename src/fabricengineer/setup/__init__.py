from typing import Any

from pyspark.sql import SparkSession


global spark_session_instance
spark_session_instance: SparkSession = None

global notebookutils_instance
notebookutils_instance = None


def set_spark(fabric_spark_session: SparkSession):
    global spark_session_instance
    spark_session_instance = fabric_spark_session


def set_notebookutils(fabric_notebookutils):
    global notebookutils_instance
    notebookutils_instance = fabric_notebookutils


def spark() -> SparkSession:
    global spark_session_instance
    return spark_session_instance


def notebookutils() -> Any:
    global notebookutils_instance
    return notebookutils_instance
