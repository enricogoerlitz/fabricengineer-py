"""
Test module for MaterializedLakeView functionality.

This module contains comprehensive tests for the MaterializedLakeView class,
including initialization, file operations, MLV creation/replacement, and refresh operations.
"""

import pytest
import os

from uuid import uuid4
from pyspark.sql import SparkSession
from tests.utils import sniff_logs, NotebookUtilsMock
from fabricengineer.transform.mlv import MaterializedLakeView


# Global variable to hold MLV instance for testing purposes
mlv: MaterializedLakeView

# Test constants for consistent test configuration
LAKEHOUSE = "Testlakehouse"
SCHEMA = "schema"
TABLE = "table"
TABLE_SUFFIX_DEFAULT = "_mlv"

# Default keyword arguments for MaterializedLakeView initialization
default_mlv_kwargs = {
    "lakehouse": LAKEHOUSE,
    "schema": SCHEMA,
    "table": TABLE,
    "table_suffix": TABLE_SUFFIX_DEFAULT,
    "spark_": None,  # Will be set during test execution
    "notebookutils_": None,  # Will be set during test execution
    "is_testing_mock": True
}


def get_default_mlv_kwargs(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> dict:
    """
    Get default keyword arguments for MaterializedLakeView initialization.

    Args:
        spark_: SparkSession instance for testing
        notebookutils_: NotebookUtilsMock instance for testing

    Returns:
        dict: Complete keyword arguments for MLV initialization
    """
    kwargs = default_mlv_kwargs.copy()
    kwargs["spark_"] = spark_
    kwargs["notebookutils_"] = notebookutils_
    return kwargs


def set_globals(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> None:
    """
    Set global variables for spark and notebookutils.

    This function is used to simulate the global environment
    that would be available in a Databricks notebook.

    Args:
        spark_: SparkSession instance to set as global
        notebookutils_: NotebookUtilsMock instance to set as global
    """
    global spark, notebookutils
    spark = spark_
    notebookutils = notebookutils_


def check_mlv_properties(
        mlv: MaterializedLakeView,
        kwargs: dict
) -> bool:
    """
    Verify that MLV instance has the expected properties based on kwargs.

    Args:
        mlv: MaterializedLakeView instance to check
        kwargs: Dictionary containing expected property values

    Returns:
        bool: True if all properties match expected values
    """
    """
    Verify that MLV instance has the expected properties based on kwargs.

    Args:
        mlv: MaterializedLakeView instance to check
        kwargs: Dictionary containing expected property values

    Returns:
        bool: True if all properties match expected values
    """
    # Extract expected values from kwargs
    lakehouse = kwargs.get("lakehouse", LAKEHOUSE)
    schema = kwargs.get("schema", SCHEMA)
    table = kwargs.get("table", TABLE)
    table_suffix = kwargs.get("table_suffix", TABLE_SUFFIX_DEFAULT)

    # Calculate derived properties
    table_name = f"{table}{table_suffix}"
    schema_path = f"{lakehouse}.{schema}"
    table_path = f"{lakehouse}.{schema}.{table_name}"
    file_path = f"Files/mlv/{lakehouse}/{schema}/{table_name}.sql.txt"

    # Verify all MLV properties match expected values
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


def test_mlv_initialization(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> None:
    """
    Test MaterializedLakeView initialization using different patterns.

    This test verifies that MLV can be initialized in three ways:
    1. Direct initialization with all parameters
    2. Initialization with chained init() method
    3. Empty initialization followed by separate init() call

    Args:
        spark_: SparkSession fixture for testing
        notebookutils_: NotebookUtilsMock fixture for testing
    """
    mlv_kwargs = get_default_mlv_kwargs(
        spark_=spark_,
        notebookutils_=notebookutils_
    )

    # Test different initialization patterns
    mlv_1 = MaterializedLakeView(**mlv_kwargs)
    mlv_2 = MaterializedLakeView().init(**mlv_kwargs)
    mlv_3 = MaterializedLakeView()
    mlv_3.init(**mlv_kwargs)

    # Verify all initialization patterns work correctly
    assert check_mlv_properties(mlv_1, mlv_kwargs)
    assert check_mlv_properties(mlv_2, mlv_kwargs)
    assert check_mlv_properties(mlv_3, mlv_kwargs)


def test_mlv_initialization_by_read_py_file(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> None:
    """
    Test MLV initialization by reading and executing the source Python file.

    This test simulates how the MLV class would be used in a notebook
    environment where the source code is read from a file and executed.

    Args:
        spark_: SparkSession fixture for testing
        notebookutils_: NotebookUtilsMock fixture for testing
    """
    set_globals(spark_=spark_, notebookutils_=notebookutils_)

    # Read and execute the MLV source code (simulating notebook import)
    with open("src/fabricengineer/transform/mlv/mlv.py") as f:
        code = f.read()
    exec(code, globals())

    # Initialize the MLV instance created by exec()
    mlv_kwargs = get_default_mlv_kwargs(
        spark_=spark_,
        notebookutils_=notebookutils_
    )
    mlv.init(**mlv_kwargs)  # noqa: F821

    # Verify the MLV instance has correct properties
    assert check_mlv_properties(mlv, mlv_kwargs)  # noqa: F821


def test_mlv_initialization_fail() -> None:
    """
    Test that MLV initialization fails appropriately when required parameters are missing.

    This test verifies that accessing properties on an uninitialized MLV
    raises appropriate ValueError exceptions with descriptive messages.
    """
    set_globals(spark_=None, notebookutils_=None)

    mlv = MaterializedLakeView()

    # Test that accessing uninitialized properties raises appropriate errors
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

    # table_suffix should not raise exception as it can be None
    _ = mlv.table_suffix


def test_mlv_to_dict() -> None:
    """
    Test the to_dict() method of MaterializedLakeView.

    This test verifies that the MLV instance can be converted to a dictionary
    containing the key properties for serialization or logging purposes.
    """
    mlv_kwargs = get_default_mlv_kwargs(spark_=None, notebookutils_=None)
    mlv = MaterializedLakeView(**mlv_kwargs)

    # Extract values for expected dictionary
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


def test__get_init_spark(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> None:
    """
    Test the _get_init_spark method for SparkSession initialization.

    This test verifies that the MLV can correctly retrieve SparkSession
    from either global variables or local parameters.

    Args:
        spark_: SparkSession fixture for testing
        notebookutils_: NotebookUtilsMock fixture for testing
    """
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv = MaterializedLakeView()

    # Test getting SparkSession from globals when None is passed
    spark_from_globals = mlv._get_init_spark(spark_=None)
    assert isinstance(spark_from_globals, SparkSession)
    assert spark_from_globals is spark_

    # Test getting SparkSession from local parameter
    local_spark = SparkSession.builder \
        .appName("LocalSession") \
        .getOrCreate()

    spark_from_local = mlv._get_init_spark(spark_=local_spark)
    assert isinstance(spark_from_local, SparkSession)
    assert spark_from_local is local_spark

    # Clean up local SparkSession
    local_spark.stop()


def test__get_init_notebookutils(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> None:
    """
    Test the _get_init_notebookutils method for NotebookUtils initialization.

    This test verifies that the MLV can correctly retrieve NotebookUtils
    from either global variables or local parameters.

    Args:
        spark_: SparkSession fixture for testing
        notebookutils_: NotebookUtilsMock fixture for testing
    """
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv = MaterializedLakeView()

    # Test getting NotebookUtils from globals when None is passed
    notebookutils_from_globals = mlv._get_init_notebookutils(notebookutils_=None)
    assert notebookutils_from_globals is notebookutils_

    # Test getting NotebookUtils from local parameter
    local_notebookutils = NotebookUtilsMock()
    notebookutils_from_local = mlv._get_init_notebookutils(
        notebookutils_=local_notebookutils
    )
    assert notebookutils_from_local is local_notebookutils
    assert notebookutils_from_local is not notebookutils_


def test_write_file(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> None:
    """
    Test the write_file method for writing SQL content to file system.

    This test verifies that SQL content can be written to the expected
    file path and that the file exists in both notebookutils and OS.

    Args:
        spark_: SparkSession fixture for testing
        notebookutils_: NotebookUtilsMock fixture for testing
    """
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs = get_default_mlv_kwargs(
        spark_=spark_,
        notebookutils_=notebookutils_
    )
    mlv = MaterializedLakeView(**mlv_kwargs)

    # Write SQL content to file
    sql = "SELECT * FROM some_table WHERE condition = true"
    mlv.write_file(sql)
    file_path = mlv.file_path

    # Verify file exists in both mock and OS file systems
    assert notebookutils_.fs.exists(file_path)
    assert os.path.exists(file_path)


def test_read_file(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> None:
    """
    Test the read_file method for reading SQL content from file system.

    This test verifies that SQL content written to a file can be
    read back correctly with matching content.

    Args:
        spark_: SparkSession fixture for testing
        notebookutils_: NotebookUtilsMock fixture for testing
    """
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs = get_default_mlv_kwargs(
        spark_=spark_,
        notebookutils_=notebookutils_
    )
    mlv = MaterializedLakeView(**mlv_kwargs)

    # Write and then read SQL content
    sql = "SELECT * FROM some_table WHERE condition = true"
    mlv.write_file(sql)
    file_content = mlv.read_file()

    # Verify content matches what was written
    assert sql == file_content


def test_create_or_replace(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> None:
    """
    Test the create_or_replace method for MLV creation and replacement scenarios.

    This comprehensive test covers multiple scenarios:
    1. Initial MLV creation when it doesn't exist
    2. No changes when SQL content is identical
    3. MLV replacement when SQL content changes
    4. Warning and recreation when file is missing but MLV exists in lakehouse

    Args:
        spark_: SparkSession fixture for testing
        notebookutils_: NotebookUtilsMock fixture for testing
    """
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs = get_default_mlv_kwargs(
        spark_=spark_,
        notebookutils_=notebookutils_
    )
    # Use unique lakehouse name to avoid conflicts between test runs
    mlv_kwargs["lakehouse"] = str(uuid4())
    mlv = MaterializedLakeView(**mlv_kwargs)

    # Prepare test SQL content
    sql = "SELECT * FROM some_table WHERE condition = true"
    sql_update = sql.replace("condition = true", "condition = false")
    assert sql != sql_update

    # Scenario 1: Create MLV when it doesn't exist
    _, logs_1_create = sniff_logs(
        lambda: mlv.create_or_replace(sql, mock_is_existing=False)
    )

    # Scenario 2: No changes when content is identical
    _, logs_2_nothing_changed = sniff_logs(
        lambda: mlv.create_or_replace(sql, mock_is_existing=True)
    )

    # Scenario 3: Replace MLV when content changes
    _, logs_3_replace = sniff_logs(
        lambda: mlv.create_or_replace(sql_update, mock_is_existing=True)
    )

    # Scenario 4: No changes when content is still identical
    _, logs_4_nothing_changed = sniff_logs(
        lambda: mlv.create_or_replace(sql_update, mock_is_existing=True)
    )

    # Scenario 5: File missing but MLV exists in lakehouse (edge case)
    # Simulate file deletion while MLV still exists in lakehouse
    os.remove(mlv.file_path)
    _, logs_5_warn_recreate = sniff_logs(
        lambda: mlv.create_or_replace(sql_update, mock_is_existing=True)
    )

    # Verify logs for Scenario 1 - Initial MLV creation
    assert len(logs_1_create) == 2
    assert "CREATE SCHEMA IF NOT EXISTS" in logs_1_create[0]
    assert "CREATE MLV" in logs_1_create[1]

    # Verify logs for Scenario 2 - No changes detected
    assert len(logs_2_nothing_changed) == 1
    assert "Nothing has changed." in logs_2_nothing_changed[0]

    # Verify logs for Scenario 3 - MLV replacement
    assert len(logs_3_replace) == 4
    assert "REPLACE MLV" in logs_3_replace[0]
    assert "DROP MATERIALIZED LAKE VIEW IF EXISTS" in logs_3_replace[1]
    assert "CREATE SCHEMA IF NOT EXISTS" in logs_3_replace[2]
    assert "CREATE MLV" in logs_3_replace[3]

    # Verify logs for Scenario 4 - No changes detected again
    assert len(logs_4_nothing_changed) == 1
    assert "Nothing has changed." in logs_4_nothing_changed[0]

    # Verify logs for Scenario 5 - Warning and recreation
    assert len(logs_5_warn_recreate) == 4
    assert "WARN: file=None, is_existing=True. RECREATE." in logs_5_warn_recreate[0]
    assert "DROP MATERIALIZED LAKE VIEW IF EXISTS" in logs_5_warn_recreate[1]
    assert "CREATE SCHEMA IF NOT EXISTS" in logs_5_warn_recreate[2]
    assert "CREATE MLV" in logs_5_warn_recreate[3]


def test_refresh(
        spark_: SparkSession,
        notebookutils_: NotebookUtilsMock
) -> None:
    """
    Test the refresh method for MLV refresh operations.

    This test verifies that MLV refresh operations work correctly
    for both incremental and full refresh modes, ensuring the
    appropriate SQL commands are executed.

    Args:
        spark_: SparkSession fixture for testing
        notebookutils_: NotebookUtilsMock fixture for testing
    """
    set_globals(spark_=spark_, notebookutils_=notebookutils_)
    mlv_kwargs = get_default_mlv_kwargs(
        spark_=spark_,
        notebookutils_=notebookutils_
    )
    # Use unique lakehouse name to avoid conflicts between test runs
    mlv_kwargs["lakehouse"] = str(uuid4())
    mlv = MaterializedLakeView(**mlv_kwargs)

    # Test incremental refresh (default behavior)
    _, logs_refresh_not_full = sniff_logs(
        lambda: mlv.refresh(full_refresh=False)
    )

    # Test full refresh operation
    _, logs_refresh_full = sniff_logs(
        lambda: mlv.refresh(full_refresh=True)
    )

    # Verify incremental refresh logs
    assert len(logs_refresh_not_full) == 1
    assert "REFRESH MATERIALIZED LAKE VIEW" in logs_refresh_not_full[0]
    assert "FULL" not in logs_refresh_not_full[0]

    # Verify full refresh logs
    assert len(logs_refresh_full) == 1
    assert "REFRESH MATERIALIZED LAKE VIEW" in logs_refresh_full[0]
    assert "FULL" in logs_refresh_full[0]
