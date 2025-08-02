import pytest

from pyspark.sql import SparkSession

from tests.utils import NotebookUtilsMock


@pytest.fixture(scope="function")
def spark_():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def notebookutils_():
    """Create a mock for NotebookUtils."""
    return NotebookUtilsMock()
