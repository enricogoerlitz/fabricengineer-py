import os
import shutil
import pytest

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from tests.utils import NotebookUtilsMock


# @pytest.fixture(scope="function")
# def spark_():
#     """Create a Spark session for testing."""
#     spark = SparkSession.builder \
#         .appName("TestSession") \
#         .master("local[*]") \
#         .getOrCreate()
#     yield spark
#     spark.stop()


@pytest.fixture(scope="function")
def spark_():
    builder = SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def notebookutils_():
    """Create a mock for NotebookUtils."""
    return NotebookUtilsMock()


@pytest.fixture(scope="session", autouse=True)
def global_cleanup_fs():
    yield  # alle Tests laufen zuerst

    print("CLEANUP: Removing temporary directories and files.")

    def cleanup_fs():
        path_tmp = "tmp"
        path_Files = "Files"

        rm_paths = [path_Files, path_tmp]
        for path in rm_paths:
            if os.path.exists(path):
                shutil.rmtree(path)

    cleanup_fs()
