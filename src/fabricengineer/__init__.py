from pyspark.sql import SparkSession


def hello() -> str:
    return "Hello from fabricengineer-py!"


def print_spark_version(spark: SparkSession) -> None:
    print(f"Spark version: {spark.version}")
