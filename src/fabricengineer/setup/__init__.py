from pyspark.sql import SparkSession


global spark
spark: SparkSession = None

global notebookutils
notebookutils = None


def set_spark(fabric_spark_session: SparkSession):
    global spark
    spark = fabric_spark_session


def set_notebookutils(fabric_notebookutils):
    global notebookutils
    notebookutils = fabric_notebookutils
