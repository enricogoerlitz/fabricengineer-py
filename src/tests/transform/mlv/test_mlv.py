import pytest
import os

from uuid import uuid4
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
    kwargs = default_mlv_kwargs.copy()
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


def test_mlv_initialization_fail():
    set_globals(spark_=None, notebookutils_=None)

    mlv = MaterializedLakeView()
    with pytest.raises(ValueError, match="Lakehouse is not initialized."):
        mlv.lakehouse
    with pytest.raises(ValueError, match="Schema is not initialized."):
        mlv.schema
    with pytest.raises(ValueError, match="Table is not initialized."):
        mlv.table
    with pytest.raises(ValueError, match="SparkSession is not initialized"):
        mlv.spark
    with pytest.raises(ValueError, match="NotebookUtils is not initialized."):
        mlv.notebookutils

    _ = mlv.table_suffix  # no exception, because suffix can be None


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


def test__get_init_spark(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv = MaterializedLakeView()

    # 'spark' from globals
    spark_from_globals = mlv._get_init_spark(spark_=None)
    assert isinstance(spark_from_globals, SparkSession)
    assert spark_from_globals is spark_

    local_spark = SparkSession.builder \
        .appName("LocalSession") \
        .getOrCreate()

    # 'spark' from local variable
    spark_from_local = mlv._get_init_spark(spark_=local_spark)
    assert isinstance(spark_from_local, SparkSession)
    assert spark_from_local is local_spark

    local_spark.stop()


def test__get_init_notebookutils(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv = MaterializedLakeView()

    # 'notebookutils' from globals
    notebookutils_from_globals = mlv._get_init_notebookutils(notebookutils_=None)
    assert notebookutils_from_globals is notebookutils_

    local_notebookutils = NotebookUtilsMock()

    # 'notebookutils' from local variable
    notebookutils_from_local = mlv._get_init_notebookutils(notebookutils_=local_notebookutils)
    assert notebookutils_from_local is local_notebookutils
    assert notebookutils_from_local is not notebookutils_


def test_write_file(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs = get_default_mlv_kwargs(spark_=spark_, notebookutils_=notebookutils_)
    mlv = MaterializedLakeView(**mlv_kwargs)

    sql = "SELECT * FROM some_table WHERE condition = true"
    mlv.write_file(sql)
    file_path = mlv.file_path

    assert notebookutils_.fs.exists(file_path)
    assert os.path.exists(file_path)


def test_read_file(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs = get_default_mlv_kwargs(spark_=spark_, notebookutils_=notebookutils_)
    mlv = MaterializedLakeView(**mlv_kwargs)

    sql = "SELECT * FROM some_table WHERE condition = true"
    mlv.write_file(sql)
    file_content = mlv.read_file()

    assert sql == file_content


def test_create_or_replace(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs = get_default_mlv_kwargs(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs["lakehouse"] = str(uuid4())
    mlv = MaterializedLakeView(**mlv_kwargs)

    sql = "SELECT * FROM some_table WHERE condition = true"
    sql_update = sql.replace("condition = true", "condition = false")
    assert sql != sql_update

    _, logs_1_create = sniff_logs(
        lambda: mlv.create_or_replace(sql, mock_is_existing=False)
    )

    _, logs_2_nothing_changed = sniff_logs(
        lambda: mlv.create_or_replace(sql, mock_is_existing=True)
    )

    _, logs_3_replace = sniff_logs(
        lambda: mlv.create_or_replace(sql_update, mock_is_existing=True)
    )

    _, logs_4_nothing_changed = sniff_logs(
        lambda: mlv.create_or_replace(sql_update, mock_is_existing=True)
    )

    # File is existing, but the MLV in Lakehouse not (maybe MLV was dropped, without removing the file)
    os.remove(mlv.file_path)
    _, logs_5_warn_recreate = sniff_logs(
        lambda: mlv.create_or_replace(sql_update, mock_is_existing=True)
    )

    # logs 1 - Create MLB
    assert len(logs_1_create) == 2
    assert "CREATE SCHEMA IF NOT EXISTS" in logs_1_create[0]
    assert "CREATE MLV" in logs_1_create[1]

    # logs 2 - Nothing changed
    assert len(logs_2_nothing_changed) == 1
    assert "Nothing has changed." in logs_2_nothing_changed[0]

    # logs 3 - Replace MLV
    assert len(logs_3_replace) == 4
    assert "REPLACE MLV" in logs_3_replace[0]
    assert "DROP MATERIALIZED LAKE VIEW IF EXISTS" in logs_3_replace[1]
    assert "CREATE SCHEMA IF NOT EXISTS" in logs_3_replace[2]
    assert "CREATE MLV" in logs_3_replace[3]

    # logs 4 - Nothing changed
    assert len(logs_4_nothing_changed) == 1
    assert "Nothing has changed." in logs_4_nothing_changed[0]

    # logs 5 - File is not existing, but the MLV in Lakehouse is
    assert len(logs_5_warn_recreate) == 4
    assert "WARN: file=None, is_existing=True. RECREATE." in logs_5_warn_recreate[0]
    assert "DROP MATERIALIZED LAKE VIEW IF EXISTS" in logs_5_warn_recreate[1]
    assert "CREATE SCHEMA IF NOT EXISTS" in logs_5_warn_recreate[2]
    assert "CREATE MLV" in logs_5_warn_recreate[3]


def test_refresh(spark_: SparkSession, notebookutils_: NotebookUtilsMock):
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs = get_default_mlv_kwargs(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs["lakehouse"] = str(uuid4())
    mlv = MaterializedLakeView(**mlv_kwargs)

    _, logs_refresh_not_full = sniff_logs(
        lambda: mlv.refresh(full_refresh=False)
    )

    _, logs_refresh_full = sniff_logs(
        lambda: mlv.refresh(full_refresh=True)
    )

    assert len(logs_refresh_not_full) == 1
    assert "REFRESH MATERIALIZED LAKE VIEW" in logs_refresh_not_full[0]
    assert "FULL" not in logs_refresh_not_full[0]

    assert len(logs_refresh_full) == 1
    assert "REFRESH MATERIALIZED LAKE VIEW" in logs_refresh_full[0]
    assert "FULL" in logs_refresh_full[0]
