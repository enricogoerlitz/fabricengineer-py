import pytest

from pyspark.sql import SparkSession
from tests.utils import sniff_logs, NotebookUtilsMock
from fabricengineer.transform.mlv import MaterializedLakeView


mlv: MaterializedLakeView

LAKEHOUSE = "Testlakehouse"
SCHEMA = "schema"
TABLE = "table"
TABLE_SUFFIX_DEFAULT = "_mlv"

default_mlv_kwargs = {
    "lakehouse": LAKEHOUSE,
    "schema": SCHEMA,
    "table": TABLE,
    "table_suffix": TABLE_SUFFIX_DEFAULT,
    "spark_": None,  # Will be set in future
    "notebookutils_": None,  # Will be set in future
    "is_testing_mock": True
}


def get_default_mlv_kwargs(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> dict:
    """Get default keyword arguments for MaterializedLakeView."""
    kwargs = {k: v for k, v in default_mlv_kwargs.items()}
    kwargs["spark_"] = spark_
    kwargs["notebookutils_"] = notebookutils_
    return kwargs


def set_globals(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    global spark, notebookutils
    spark = spark_
    notebookutils = notebookutils_


def check_mlv_properties(mlv: MaterializedLakeView, kwargs: dict) -> None:
    lakehouse = kwargs.get("lakehouse", LAKEHOUSE)
    schema = kwargs.get("schema", SCHEMA)
    table = kwargs.get("table", TABLE)
    table_suffix = kwargs.get("table_suffix", TABLE_SUFFIX_DEFAULT)
    table_name = f"{table}{table_suffix}"
    schema_path = f"{lakehouse}.{schema}"
    table_path = f"{lakehouse}.{schema}.{table_name}"
    file_path = f"Files/mlv/{lakehouse}/{schema}/{table_name}.sql.txt"

    assert mlv._is_testing_mock is True
    assert mlv.lakehouse == lakehouse
    assert mlv.schema == schema
    assert mlv.table == table
    assert mlv.table_suffix == table_suffix
    assert mlv.table_name == table_name
    assert mlv.schema_path == schema_path
    assert mlv.table_path == table_path
    assert mlv.file_path == file_path
    assert isinstance(mlv.spark, SparkSession)
    assert mlv.notebookutils is not None
    return True


def test_mlv_initialization(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    mlv_kwargs = get_default_mlv_kwargs(spark_=spark_, notebookutils_=notebookutils_)

    mlv_1 = MaterializedLakeView(**mlv_kwargs)
    mlv_2 = MaterializedLakeView().init(**mlv_kwargs)
    mlv_3 = MaterializedLakeView()
    mlv_3.init(**mlv_kwargs)

    assert check_mlv_properties(mlv_1, mlv_kwargs)
    assert check_mlv_properties(mlv_2, mlv_kwargs)
    assert check_mlv_properties(mlv_3, mlv_kwargs)


def test_mlv_initialization_by_read_py_file(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    set_globals(spark_=spark_, notebookutils_=notebookutils_)

    with open("src/fabricengineer/transform/mlv/mlv.py") as f:
        code = f.read()
    exec(code, globals())

    mlv_kwargs = get_default_mlv_kwargs(spark_=spark_, notebookutils_=notebookutils_)
    mlv.init(**mlv_kwargs)  # noqa: F821

    assert check_mlv_properties(mlv, mlv_kwargs)  # noqa: F821


def test_mlv_initialization_fail(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    set_globals(spark_=None, notebookutils_=None)

    mlv_kwargs = get_default_mlv_kwargs(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs_spark_missing = {k: v for k, v in mlv_kwargs.items()}
    mlv_kwargs_notebook_utils_missing = {k: v for k, v in mlv_kwargs.items()}
    mlv_kwargs_spark_missing["spark_"] = None
    mlv_kwargs_notebook_utils_missing["notebookutils_"] = None

    mlv_spark_missing = MaterializedLakeView(**mlv_kwargs_spark_missing)
    mlv_notebook_utils_missing = MaterializedLakeView(**mlv_kwargs_notebook_utils_missing)
    with pytest.raises(ValueError, match="SparkSession is not initialized"):
        mlv_spark_missing.spark

    with pytest.raises(ValueError, match="NotebookUtils is not initialized."):
        mlv_notebook_utils_missing.notebookutils


def test_mlv_to_dict():
    mlv_kwargs = get_default_mlv_kwargs(spark_=None, notebookutils_=None)
    mlv = MaterializedLakeView(**mlv_kwargs)

    lakehouse = mlv_kwargs.get("lakehouse")
    schema = mlv_kwargs.get("schema")
    table = mlv_kwargs.get("table")
    table_suffix = mlv_kwargs.get("table_suffix")
    expected_dict = {
        "lakehouse": lakehouse,
        "schema": schema,
        "table": table,
        "table_path": f"{lakehouse}.{schema}.{table}{table_suffix}"
    }

    assert mlv.to_dict() == expected_dict


def test_write_file(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    sniff_logs(lambda: "")

# am ende auch testen, ob man die mlv mit exec reinholen kann!
