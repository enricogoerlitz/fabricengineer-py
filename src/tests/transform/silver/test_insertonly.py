"""
Test module for SilverIngestionInsertOnlyService functionality.

This module contains comprehensive tests for the SilverIngestionInsertOnlyService class,
which handles ETL operations for insert-only data ingestion from bronze to silver layer
in a lakehouse architecture. The tests cover various scenarios including:

- Service initialization and parameter validation
- Data ingestion operations (insert, update, delete)
- Schema evolution (adding/removing columns)
- Data transformations
- Column filtering for comparison operations
- Delta load processing
- Historization features
- Materialized Lake View (MLV) generation
- Constant columns and partitioning

Run tests with: pytest src/tests/transform/silver -v
"""

from venv import logger
import pytest

from datetime import datetime
from uuid import uuid4
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F
)

from tests.transform.silver.utils import (
    BronzeDataFrameRecord,
    BronzeDataFrameDataGenerator
)
from fabricengineer.transform.silver.utils import ConstantColumn
from fabricengineer.transform.silver.insertonly import (
    SilverIngestionInsertOnlyService
)
from fabricengineer.transform.lakehouse import LakehouseTable


# Test constant for additional column name
NCOL = "ncol"

# Default ETL configuration for consistent test setup
default_etl_kwargs = {
    "spark_": None,  # Will be set during test execution
    "source_table": None,  # Will be set during test execution
    "destination_table": None,  # Will be set during test execution
    "nk_columns": ["id"],  # Natural key columns for record identification
    "constant_columns": [],  # Additional constant columns to add
    "is_delta_load": False,  # Whether to process as delta load
    "delta_load_use_broadcast": True,  # Use broadcast join for delta loads
    "transformations": {},  # Custom transformation functions
    "exclude_comparing_columns": [],  # Columns to exclude from comparison
    "include_comparing_columns": [],  # Columns to include in comparison
    "historize": True,  # Enable historization of data changes
    "partition_by_columns": [],  # Columns for data partitioning
    "df_bronze": None,  # Custom bronze DataFrame (optional)
    "create_historized_mlv": True,  # Create materialized lake view for history
    "nk_column_concate_str": "_",  # Separator for natural key concatenation
    "is_testing_mock": True  # Enable testing mock mode
}


def get_default_etl_kwargs(spark_: SparkSession) -> dict:
    """
    Generate default ETL configuration for testing.

    Creates consistent test configuration with randomized table names
    to avoid conflicts between test runs.

    Args:
        spark_: SparkSession instance for testing

    Returns:
        dict: Complete ETL configuration dictionary
    """
    # Create source table configuration with unique name
    source_table = LakehouseTable(
        lakehouse="BronzeLakehouse",
        schema="default_schema",
        table=str(uuid4())
    )

    # Create destination table configuration (same schema and table as source)
    dest_table = LakehouseTable(
        lakehouse="SilverLakehouse",
        schema=source_table.schema,
        table=source_table.table
    )

    # Build complete configuration
    kwargs = default_etl_kwargs.copy()
    kwargs["spark_"] = spark_
    kwargs["source_table"] = source_table
    kwargs["destination_table"] = dest_table
    return kwargs


def test_init_etl(spark_: SparkSession) -> None:
    """
    Test the initialization of SilverIngestionInsertOnlyService.

    This test verifies that the ETL service can be properly initialized
    with all required parameters and that all internal state is set correctly.
    It also tests the initialization with constant columns.

    Args:
        spark_: SparkSession fixture for testing
    """
    # Setup ETL configuration with constant columns
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["constant_columns"] = [
        ConstantColumn(name="instance", value="VTSD", part_of_nk=True),
        ConstantColumn(name="other", value="column")
    ]
    etl = SilverIngestionInsertOnlyService()

    # Initialize the service
    etl.init(**etl_kwargs)

    # Verify basic initialization state
    assert etl._is_initialized is True
    assert etl._spark == spark_
    assert etl._src_table == etl_kwargs["source_table"]
    assert etl._dest_table == etl_kwargs["destination_table"]

    # Verify column configuration
    assert etl._nk_columns == etl_kwargs["nk_columns"]
    assert etl._constant_columns == etl_kwargs["constant_columns"]

    # Verify load configuration
    assert etl._is_delta_load == etl_kwargs["is_delta_load"]
    assert etl._delta_load_use_broadcast == etl_kwargs["delta_load_use_broadcast"]
    assert etl._transformations == etl_kwargs["transformations"]

    # Verify comparison column configuration (includes automatic exclusions)
    expected_excluded_columns = set([
        "id", "PK", "NK", "ROW_DELETE_DTS", "ROW_LOAD_DTS",
        "OTHER", "INSTANCE"
    ] + etl_kwargs["exclude_comparing_columns"])
    assert etl._exclude_comparing_columns == expected_excluded_columns
    assert etl._include_comparing_columns == etl_kwargs["include_comparing_columns"]

    # Verify historization and partitioning
    assert etl._historize == etl_kwargs["historize"]
    assert etl._partition_by == etl_kwargs["partition_by_columns"]
    assert etl._df_bronze is None
    assert etl._is_create_hist_mlv == etl_kwargs["create_historized_mlv"]
    assert etl._is_testing_mock == etl_kwargs["is_testing_mock"]

    # Verify MLV configuration
    assert etl.mlv_code is None
    expected_mlv_name = (
        f"{etl._dest_table.lakehouse}.{etl._dest_table.schema}."
        f"{etl._dest_table.table}_h"
    )
    assert etl.mlv_name == expected_mlv_name

    # Verify data warehouse columns are properly configured
    assert len(etl._dw_columns) == 4
    assert etl._dw_columns[0] == etl._pk_column_name
    assert etl._dw_columns[1] == etl._nk_column_name
    assert etl._dw_columns[2] == etl._row_delete_dts_column
    assert etl._dw_columns[3] == etl._row_load_dts_column


def test_init_etl_fail_params(spark_: SparkSession) -> None:
    """
    Test that ETL initialization fails appropriately with invalid parameters.

    This comprehensive test verifies that the ETL service properly validates
    all input parameters and raises appropriate errors with descriptive
    messages when invalid values are provided.

    Args:
        spark_: SparkSession fixture for testing
    """
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()

    # Test type validation for DataFrame parameters
    with pytest.raises(TypeError, match=f"should be type of {DataFrame.__name__}"):
        kwargs = etl_kwargs.copy() | {"df_bronze": "str"}
        etl.init(**kwargs)

    # Test type validation for SparkSession parameter
    with pytest.raises(TypeError, match=f"should be type of {SparkSession.__name__}"):
        kwargs = etl_kwargs.copy() | {"spark_": "str"}
        etl.init(**kwargs)

    # Test type validation for boolean parameters
    with pytest.raises(TypeError, match=f"should be type of {bool.__name__}"):
        kwargs = etl_kwargs.copy() | {"historize": "str"}
        etl.init(**kwargs)

    with pytest.raises(TypeError, match=f"should be type of {bool.__name__}"):
        kwargs = etl_kwargs.copy() | {"create_historized_mlv": "str"}
        etl.init(**kwargs)

    # is_delta_load should be bool
    with pytest.raises(TypeError, match=f"should be type of {bool.__name__}"):
        kwargs = etl_kwargs.copy() | {"is_delta_load": "str"}
        etl.init(**kwargs)

    # delta_load_use_broadcast should be bool
    with pytest.raises(TypeError, match=f"should be type of {bool.__name__}"):
        kwargs = etl_kwargs.copy() | {"delta_load_use_broadcast": "str"}
        etl.init(**kwargs)

    # transformations should be dict
    with pytest.raises(TypeError, match=f"should be type of {dict.__name__}"):
        kwargs = etl_kwargs.copy() | {"transformations": "str"}
        etl.init(**kwargs)

    # source_table should be LakehouseTable
    with pytest.raises(TypeError, match=f"should be type of {LakehouseTable.__name__}"):
        kwargs = etl_kwargs.copy() | {"source_table": "str"}
        etl.init(**kwargs)

    # destination_table should be LakehouseTable
    with pytest.raises(TypeError, match=f"should be type of {LakehouseTable.__name__}"):
        kwargs = etl_kwargs.copy() | {"destination_table": "str"}
        etl.init(**kwargs)

    # include_comparing_columns should be list
    with pytest.raises(TypeError, match=f"should be type of {list.__name__}"):
        kwargs = etl_kwargs.copy() | {"include_comparing_columns": "str"}
        etl.init(**kwargs)

    # exclude_comparing_columns should be list
    with pytest.raises(TypeError, match=f"should be type of {list.__name__}"):
        kwargs = etl_kwargs.copy() | {"exclude_comparing_columns": "str"}
        etl.init(**kwargs)

    # partition_by_columns should be list
    with pytest.raises(TypeError, match=f"should be type of {list.__name__}"):
        kwargs = etl_kwargs.copy() | {"partition_by_columns": "str"}
        etl.init(**kwargs)

    # pk_column_name should be str
    with pytest.raises(TypeError, match=f"should be type of {str.__name__}"):
        kwargs = etl_kwargs.copy() | {"pk_column_name": 123}
        etl.init(**kwargs)

    # nk_column_name should be str
    with pytest.raises(TypeError, match=f"should be type of {str.__name__}"):
        kwargs = etl_kwargs.copy() | {"nk_column_name": 123}
        etl.init(**kwargs)

    # nk_columns should be list
    with pytest.raises(TypeError, match=f"should be type of {list.__name__}"):
        kwargs = etl_kwargs.copy() | {"nk_columns": "str"}
        etl.init(**kwargs)

    # nk_column_concate_str should be str
    with pytest.raises(TypeError, match=f"should be type of {str.__name__}"):
        kwargs = etl_kwargs.copy() | {"nk_column_concate_str": 123}
        etl.init(**kwargs)

    # mlv_suffix should be str
    with pytest.raises(TypeError, match=f"should be type of {str.__name__}"):
        kwargs = etl_kwargs.copy() | {"mlv_suffix": 123}
        etl.init(**kwargs)

    # constant_columns should be list
    with pytest.raises(TypeError, match=f"should be type of {list.__name__}"):
        kwargs = etl_kwargs.copy() | {"constant_columns": "str"}
        etl.init(**kwargs)

    # row_load_dts_column should be str
    with pytest.raises(TypeError, match=f"should be type of {str.__name__}"):
        kwargs = etl_kwargs.copy() | {"row_load_dts_column": 123}
        etl.init(**kwargs)

    # row_hist_number_column should be str
    with pytest.raises(TypeError, match=f"should be type of {str.__name__}"):
        kwargs = etl_kwargs.copy() | {"row_hist_number_column": 123}
        etl.init(**kwargs)

    # row_is_current_column should be str
    with pytest.raises(TypeError, match=f"should be type of {str.__name__}"):
        kwargs = etl_kwargs.copy() | {"row_is_current_column": 123}
        etl.init(**kwargs)

    # row_update_dts_column should be str
    with pytest.raises(TypeError, match=f"should be type of {str.__name__}"):
        kwargs = etl_kwargs.copy() | {"row_update_dts_column": 123}
        etl.init(**kwargs)

    # row_delete_dts_column should be str
    with pytest.raises(TypeError, match=f"should be type of {str.__name__}"):
        kwargs = etl_kwargs.copy() | {"row_delete_dts_column": 123}
        etl.init(**kwargs)

    # pk_column_name should be min length 2
    with pytest.raises(ValueError, match="Param length to short."):
        kwargs = etl_kwargs.copy() | {"pk_column_name": "a"}
        etl.init(**kwargs)

    # nk_column_name should be min length 2
    with pytest.raises(ValueError, match="Param length to short."):
        kwargs = etl_kwargs.copy() | {"nk_column_name": "a"}
        etl.init(**kwargs)

    # source_table.lakehouse should be min length 3
    with pytest.raises(ValueError, match="Param length to short."):
        table = LakehouseTable(lakehouse="ab", schema="default_schema", table="test_table")
        kwargs = etl_kwargs.copy() | {"source_table": table}
        etl.init(**kwargs)

    # source_table.table should be min length 3
    with pytest.raises(ValueError, match="Param length to short."):
        table = LakehouseTable(lakehouse="BronzeLakehouse", schema="default_schema", table="ab")
        kwargs = etl_kwargs.copy() | {"source_table": table}
        etl.init(**kwargs)

    # source_table.schema should be min length 1
    with pytest.raises(ValueError, match="Param length to short."):
        table = LakehouseTable(lakehouse="BronzeLakehouse", schema="", table="test_table")
        kwargs = etl_kwargs.copy() | {"source_table": table}
        etl.init(**kwargs)

    # destination_table.lakehouse should be min length 3
    with pytest.raises(ValueError, match="Param length to short."):
        table = LakehouseTable(lakehouse="ab", schema="default_schema", table="test_table")
        kwargs = etl_kwargs.copy() | {"destination_table": table}
        etl.init(**kwargs)

    # destination_table.schema should be min length 1
    with pytest.raises(ValueError, match="Param length to short."):
        table = LakehouseTable(lakehouse="SilverLakehouse", schema="", table="test_table")
        kwargs = etl_kwargs.copy() | {"destination_table": table}
        etl.init(**kwargs)

    # destination_table.table should be min length 3
    with pytest.raises(ValueError, match="Param length to short."):
        table = LakehouseTable(lakehouse="SilverLakehouse", schema="default_schema", table="ab")
        kwargs = etl_kwargs.copy() | {"destination_table": table}
        etl.init(**kwargs)

    # nk_columns should be min length 1
    with pytest.raises(ValueError, match="Param length to short."):
        kwargs = etl_kwargs.copy() | {"nk_columns": []}
        etl.init(**kwargs)

    # nk_column_concate_str should be min length 1
    with pytest.raises(ValueError, match="Param length to short."):
        kwargs = etl_kwargs.copy() | {"nk_column_concate_str": ""}
        etl.init(**kwargs)

    # mlv_suffix should be min length 1
    with pytest.raises(ValueError, match="Param length to short."):
        kwargs = etl_kwargs.copy() | {"mlv_suffix": ""}
        etl.init(**kwargs)

    # row_load_dts_column should be min length 3
    with pytest.raises(ValueError, match="Param length to short."):
        kwargs = etl_kwargs.copy() | {"row_load_dts_column": "ab"}
        etl.init(**kwargs)

    # row_hist_number_column should be min length 3
    with pytest.raises(ValueError, match="Param length to short."):
        kwargs = etl_kwargs.copy() | {"row_hist_number_column": "ab"}
        etl.init(**kwargs)

    # row_is_current_column should be min length 3
    with pytest.raises(ValueError, match="Param length to short."):
        kwargs = etl_kwargs.copy() | {"row_is_current_column": "ab"}
        etl.init(**kwargs)

    # row_update_dts_column should be min length 3
    with pytest.raises(ValueError, match="Param length to short."):
        kwargs = etl_kwargs.copy() | {"row_update_dts_column": "ab"}
        etl.init(**kwargs)

    # row_delete_dts_column should be min length 3
    with pytest.raises(ValueError, match="Param length to short."):
        kwargs = etl_kwargs.copy() | {"row_delete_dts_column": "ab"}
        etl.init(**kwargs)

    # transformations should be callable
    with pytest.raises(TypeError, match="is not callable"):
        kwargs = etl_kwargs.copy() | {"transformations": {
            "test_transformation": "not_callable"
        }}
        etl.init(**kwargs)

    # constant_columns should be list of ConstantColumn
    with pytest.raises(TypeError, match=f"should be type of {ConstantColumn.__name__}"):
        kwargs = etl_kwargs.copy() | {"constant_columns": ["not_a_constant_column"]}
        etl.init(**kwargs)


def test_ingest(spark_: SparkSession) -> None:
    """
    Test the core ingestion functionality of SilverIngestionInsertOnlyService.

    This comprehensive test covers the complete ingestion workflow including:
    1. Initial data load from bronze to silver layer
    2. No-change detection on subsequent runs
    3. Complex data changes (inserts, updates, deletes)
    4. Proper historization of data changes
    5. Correct handling of delete operations with timestamps

    The test validates that:
    - All data warehouse columns are properly added
    - Row counts are accurate throughout the process
    - Data integrity is maintained during transformations
    - Deleted records are properly marked with ROW_DELETE_DTS

    Args:
    """
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Setup test data configuration
    prefix = "Name-"
    init_count = 10

    # Create initial test records
    init_data = [
        BronzeDataFrameRecord(id=i, name=f"{prefix}{i}")
        for i in range(1, init_count + 1)
    ]
    current_expected_data = [r for r in init_data]

    # Initialize bronze data generator and write initial data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data,
        init_name_prefix=prefix
    )

    bronze.write().read()

    # Verify initial bronze data is correct
    for i, row in enumerate(bronze.df.orderBy("id").collect()):
        assert row["name"] == init_data[i].name

    # PHASE 1: Initial silver ingestion (first load)
    inserted_df = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify initial ingestion results
    assert inserted_df is not None
    assert inserted_df.count() == len(current_expected_data)
    assert bronze.df.count() == len(current_expected_data)
    assert silver_df_1.count() == len(current_expected_data)

    # Verify column structure is preserved and DW columns are added
    assert all(True for column in bronze.df.columns if column in inserted_df.columns)
    assert all(True for column in etl._dw_columns if column in inserted_df.columns)
    assert all(True for column in bronze.df.columns if column in silver_df_1.columns)
    assert all(True for column in etl._dw_columns if column in silver_df_1.columns)

    # Verify data content integrity in initial load
    for i, row in enumerate(inserted_df.orderBy("id").collect()):
        assert row["name"] == init_data[i].name
        assert row["created_at"] == init_data[i].created_at
        assert row["updated_at"] == init_data[i].updated_at

    for i, row in enumerate(silver_df_1.orderBy("id").collect()):
        assert row["name"] == current_expected_data[i].name
        assert row["created_at"] == current_expected_data[i].created_at
        assert row["updated_at"] == current_expected_data[i].updated_at

    # PHASE 2: No-change ingestion (should detect no changes)
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    # Verify no changes detected (empty inserted_df)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == 0
    assert silver_df_2.count() == len(current_expected_data)
    assert all(True for column in bronze.df.columns if column in inserted_df.columns)
    assert all(True for column in etl._dw_columns if column in inserted_df.columns)

    # PHASE 3: Complex changes (inserts, updates, deletes)
    # Prepare new records for insertion
    new_data = [
        BronzeDataFrameRecord(id=100, name="Name-100"),
        BronzeDataFrameRecord(id=101, name="Name-101"),
        BronzeDataFrameRecord(id=102, name="Name-102"),
        BronzeDataFrameRecord(id=103, name="Name-103"),
    ]
    current_expected_data += new_data

    # Prepare records for updates (modify existing records)
    updated_data_ids = [4, 5, 6]
    updated_data = [
        BronzeDataFrameRecord(
            id=r.id,
            name=f"{r.name}-Update-1",
            created_at=r.created_at
        )
        for r in current_expected_data
        if r.id in updated_data_ids
    ]
    current_expected_data += updated_data

    # Prepare records for deletion (remove from bronze, track in silver)
    deleted_data_ids = [1, 7, 9]
    deleted_dt_for_reference = datetime.now()
    deleted_data = [
        BronzeDataFrameRecord(
            id=r.id,
            name=r.name,
            created_at=r.created_at,
            updated_at=deleted_dt_for_reference
        )
        for r in current_expected_data if r.id in deleted_data_ids
    ]
    current_expected_data += deleted_data

    # Apply all changes to bronze layer (inserts, updates, deletes)
    bronze.add_records(new_data) \
          .update_records(updated_data) \
          .delete_records(deleted_data_ids) \
          .write() \
          .read()

    # Execute ingestion after complex changes
    inserted_df_3 = etl.ingest()
    silver_df_3 = etl.read_silver_df()

    # Calculate expected change count
    changed_count = len(new_data) + len(updated_data) + len(deleted_data_ids)

    # Sort expected data for consistent comparison
    current_expected_data = sorted(
        current_expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    assert bronze.df.count() == init_count + len(new_data) - len(deleted_data_ids)
    assert inserted_df_3 is not None
    assert inserted_df_3.count() == changed_count
    assert silver_df_3.count() == len(current_expected_data)

    deleted_count = 0
    for i, row in enumerate(silver_df_3.orderBy(F.col("id").asc(), F.col("ROW_LOAD_DTS").asc()).collect()):
        logger.info(f"Row {i}: {row}")
        expected_record = current_expected_data[i]
        assert row["id"] == expected_record.id
        assert row["name"] == expected_record.name
        assert row["created_at"] == expected_record.created_at

        is_deleted_row = (
            row["id"] in deleted_data_ids and
            expected_record.updated_at == deleted_dt_for_reference
        )

        if is_deleted_row:
            assert row["ROW_DELETE_DTS"] is not None
            deleted_count += 1
        else:
            assert row["ROW_DELETE_DTS"] is None

    assert deleted_count == len(deleted_data_ids)


def test_ingest_new_added_column(spark_: SparkSession) -> None:
    """
    Test schema evolution when new columns are added to the source data.

    This test verifies that the ETL service can handle schema changes
    gracefully when new columns appear in the bronze layer. It ensures:
    - New columns are automatically detected and included
    - Existing data is preserved with NULL values for new columns
    - The ingestion process continues to work correctly
    - Schema alignment between bronze and silver layers

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Initial setup and baseline data ingestion
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Create initial dataset without the new column
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]
    expected_data = [r for r in init_data]

    # Set up bronze layer with initial data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    # 1. Perform initial ingestion to establish baseline
    inserted_df_1 = etl.ingest()
    silver_df = etl.read_silver_df()

    # Verify initial state - no new column exists yet
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df.count() == len(init_data)
    assert NCOL not in bronze.df.columns
    assert NCOL not in inserted_df_1.columns
    assert NCOL not in silver_df.columns

    # PHASE 2: Schema evolution - adding new column with data
    # 1. Add new column
    new_data = [
        BronzeDataFrameRecord(id=11, name="Name-11", ncol="Value-11"),
        BronzeDataFrameRecord(id=12, name="Name-12", ncol="Value-12")
    ]
    updated_data = [
        BronzeDataFrameRecord(id=1, name="Name-1", ncol="Value-1")
    ]
    expected_data += new_data
    expected_data += updated_data

    # Add the new column to bronze schema and populate with data
    bronze.add_ncol_column() \
          .add_records(new_data) \
          .write() \
          .read()

    # Sort expected data for comparison consistency
    expected_data = sorted(
        expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    # 2. Perform second ingestion with new column present
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    # Verify schema evolution was handled correctly
    assert bronze.df.count() == 4
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == 2  # Only new records should be inserted
    assert silver_df_2.count() == 4     # Total records in silver layer
    assert NCOL in bronze.df.columns    # New column exists in bronze
    assert NCOL in inserted_df_2.columns  # New column exists in inserted data
    assert NCOL in silver_df_2.columns    # New column exists in silver layer

    # 3. Validate that new records contain correct values for new column
    for i, row in enumerate(inserted_df_2.orderBy(F.col("id").asc(), F.col("ROW_LOAD_DTS").asc()).collect()):
        assert row["name"] == new_data[i].name
        assert row["ncol"] == new_data[i].ncol


def test_ingest_remove_column(spark_: SparkSession) -> None:
    """
    Test schema evolution when columns are removed from the source data.

    This test verifies that the ETL service handles column removal scenarios:
    - Removed columns are preserved in silver layer with NULL values
    - Data integrity is maintained for existing columns
    - No data loss occurs during schema changes
    - Proper handling of missing column values in new records

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Initial setup with column present
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Create initial data that includes the column to be removed later
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1", ncol="Value-1"),
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="Value-2")
    ]
    expected_data = [r for r in init_data]

    # Set up bronze layer with the additional column
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=[]
    )
    bronze.add_ncol_column() \
          .add_records(init_data) \
          .write() \
          .read()

    # 1. Initial ingestion with column present
    inserted_df_1 = etl.ingest()
    silver_df = etl.read_silver_df()

    # Verify initial state - column exists everywhere
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df.count() == len(init_data)
    assert NCOL in bronze.df.columns
    assert NCOL in inserted_df_1.columns
    assert NCOL in silver_df.columns

    # Validate initial data with non-null values in the column
    for i, row in enumerate(inserted_df_1.orderBy(F.col("id").asc(), F.col("ROW_LOAD_DTS").asc()).collect()):
        assert row["id"] == init_data[i].id
        assert row["name"] == init_data[i].name
        assert row["ncol"] == init_data[i].ncol
        assert row["ncol"] is not None

    # PHASE 2: Schema evolution - removing column from bronze
    # 1. Remove column
    bronze.remove_ncol_column()

    # Prepare new data without the removed column
    new_data = [
        BronzeDataFrameRecord(id=11, name="Name-11"),
        BronzeDataFrameRecord(id=12, name="Name-12")
    ]
    expected_data += new_data

    # Also update existing record (without the removed column)
    updated_data = [
        BronzeDataFrameRecord(id=1, name="Name-Updated")
    ]
    expected_data += updated_data

    # Add new records and update existing ones
    bronze.add_records(new_data) \
          .update_records(updated_data) \
          .write() \
          .read()

    # 2. Perform ingestion after column removal
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    # Sort expected data for comparison consistency
    expected_data = sorted(
        expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    # Verify column removal was handled correctly
    assert bronze.df.count() == len(init_data) + len(new_data)  # Bronze has fewer records (no column)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(new_data) + len(updated_data)  # Only changed records
    assert silver_df_2.count() == len(expected_data)  # All records preserved in silver
    assert NCOL not in bronze.df.columns    # Column removed from bronze
    assert NCOL in inserted_df_2.columns    # Column preserved in silver (with NULLs)
    assert NCOL in silver_df_2.columns      # Column preserved in silver layer

    # 3. Validate that all expected data is present with correct column values
    for i, row in enumerate(silver_df_2.orderBy(F.col("id").asc(), F.col("ROW_LOAD_DTS").asc()).collect()):
        assert row["id"] == expected_data[i].id
        assert row["name"] == expected_data[i].name
        assert row["ncol"] == expected_data[i].ncol  # May be NULL for new records


def test_ingest_reactivating_deleted_values_in_source(
        spark_: SparkSession
) -> None:
    """
    Test the reactivation of previously deleted records in the source system.

    This test covers a complex scenario where records are:
    1. Initially loaded into the silver layer
    2. Deleted from the source (marked as deleted in silver)
    3. Re-added to the source system (reactivated)

    Verifies that:
    - Deleted records are properly marked with ROW_DELETE_DTS
    - Reactivated records create new entries in silver
    - Historical data is preserved throughout the process
    - Multiple versions of the same record are maintained correctly

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Initial data setup and baseline ingestion
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Create initial dataset with 5 records
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2"),
        BronzeDataFrameRecord(id=3, name="Name-3"),
        BronzeDataFrameRecord(id=4, name="Name-4"),
        BronzeDataFrameRecord(id=5, name="Name-5")
    ]
    expected_data = [r for r in init_data]

    # Set up bronze layer with initial data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    # 1. Init silver ingestion - establish baseline
    inserted_df = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify initial ingestion completed successfully
    assert inserted_df is not None
    assert inserted_df.count() == len(expected_data)
    assert bronze.df.count() == len(expected_data)
    assert silver_df_1.count() == len(expected_data)

    # PHASE 2: Record deletion simulation
    # 2. delete some records
    deleted_data_ids = [1, 3, 5]  # Delete records with IDs 1, 3, and 5
    bronze.delete_records(deleted_data_ids) \
          .write() \
          .read()

    # Perform ingestion after deletions
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    # Track deleted records in expected data (they get marked as deleted, not removed)
    expected_data += [r for r in init_data if r.id in deleted_data_ids]

    # Verify deletion handling
    assert bronze.df.count() == len(init_data) - len(deleted_data_ids)  # Bronze has fewer records
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(deleted_data_ids)  # Deleted records inserted with delete timestamp
    assert silver_df_2.count() == len(expected_data)  # Silver preserves all records

    # PHASE 3: Record reactivation scenario
    # 3. reactivating deleted records in source
    reactivated_data_ids = [1, 5]  # Reactivate subset of previously deleted records
    reactivated_data = [r for r in init_data if r.id in reactivated_data_ids]

    # Add reactivated records back to bronze layer
    bronze.add_records(reactivated_data) \
          .write() \
          .read()

    # Perform ingestion after reactivation
    inserted_df_3 = etl.ingest()
    silver_df_3 = etl.read_silver_df()

    # Track reactivated records in expected data
    expected_data += [r for r in init_data if r.id in reactivated_data_ids]

    # Verify reactivation handling
    assert len(reactivated_data_ids) > 0
    assert bronze.df.count() == len(init_data) - len(deleted_data_ids) + len(reactivated_data_ids)
    assert inserted_df_3 is not None
    assert inserted_df_3.count() == len(reactivated_data_ids)  # Only reactivated records inserted
    assert silver_df_3.count() == len(expected_data)  # All records including reactivated ones

    # PHASE 4: Validate reactivation behavior for each reactivated record
    for del_id in reactivated_data_ids:
        print("Testing deleted id:", del_id)
        df_test_deletes = silver_df_3.filter(F.col("id") == del_id)

        # Each reactivated ID should have 3 records: original, deleted, reactivated
        assert df_test_deletes.count() == 3
        assert df_test_deletes.filter(F.col("ROW_DELETE_DTS").isNull()).count() == 2  # Original + reactivated
        assert df_test_deletes.filter(F.col("ROW_DELETE_DTS").isNotNull()).count() == 1  # Deleted record

        # Most recent record should be active (no delete timestamp)
        assert df_test_deletes.orderBy(F.col("ROW_LOAD_DTS").desc()).collect()[0]["ROW_DELETE_DTS"] is None


def test_ingest_with_transformations(spark_: SparkSession) -> None:
    """
    Test data ingestion with custom transformation functions.

    This test verifies that custom transformation functions can be applied
    to specific tables during the ingestion process. It ensures:
    - Transformation functions are correctly applied to target tables
    - Data is properly modified according to transformation logic
    - Transformations work seamlessly with the ETL pipeline
    - Original data in bronze layer remains unchanged

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup transformation function and ETL configuration
    prefix = "Transformed-"

    # Define custom transformation function to modify name column
    def transform_table(df: DataFrame, etl) -> DataFrame:
        df = df.withColumn("name", F.concat(F.lit(prefix), F.col("name")))
        return df

    # Configure ETL with table-specific transformation
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["transformations"] = {
        etl_kwargs["source_table"].table: transform_table  # Apply to specific table
    }

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # PHASE 2: Prepare test data and expected results
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]
    # Expected data should have transformed names with prefix
    expected_data = [
        BronzeDataFrameRecord(id=r.id, name=f"{prefix}{r.name}")
        for r in
        init_data
    ]

    # Set up bronze layer with original data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    # PHASE 3: Execute ingestion with transformations
    inserted_df = etl.ingest()
    silver_df = etl.read_silver_df()

    # Verify ingestion completed successfully
    assert bronze.df.count() == len(init_data)
    assert inserted_df is not None
    assert inserted_df.count() == len(init_data)
    assert silver_df.count() == len(init_data)

    # PHASE 4: Validate transformation was applied correctly
    for i, row in enumerate(inserted_df.orderBy("id").collect()):
        assert row["name"] == expected_data[i].name  # Check transformed names


def test_ingest_with_transformation_star(spark_: SparkSession) -> None:
    """
    Test universal transformations applied to all tables using wildcard (*).

    This test verifies that transformation functions can be applied to all
    tables using the "*" wildcard pattern. It ensures:
    - Universal transformations are applied to any table
    - Wildcard pattern matching works correctly
    - All tables receive the same transformation logic

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup universal transformation with wildcard pattern
    prefix = "Transformed-"

    # Define transformation function to be applied to all tables
    def transform_table(df: DataFrame, etl) -> DataFrame:
        df = df.withColumn("name", F.concat(F.lit(prefix), F.col("name")))
        return df

    # Configure ETL with wildcard transformation (applies to all tables)
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["transformations"] = {
        "*": transform_table  # Wildcard pattern for universal application
    }

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # PHASE 2: Prepare test data and expected results
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]
    # Expected data should have transformed names with prefix
    expected_data = [
        BronzeDataFrameRecord(id=r.id, name=f"{prefix}{r.name}")
        for r in
        init_data
    ]

    # Set up bronze layer with original data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    # PHASE 3: Execute ingestion with universal transformations
    inserted_df = etl.ingest()
    silver_df = etl.read_silver_df()

    # Verify ingestion completed successfully
    assert bronze.df.count() == len(init_data)
    assert inserted_df is not None
    assert inserted_df.count() == len(init_data)
    assert silver_df.count() == len(init_data)

    # PHASE 4: Validate universal transformation was applied
    for i, row in enumerate(inserted_df.orderBy("id").collect()):
        assert row["name"] == expected_data[i].name  # Check transformed names


def test_ingest_with_transformation_not_applied(
        spark_: SparkSession
) -> None:
    """
    Test that transformations are not applied when table names don't match.

    This test verifies that transformation functions are only applied when
    the table name matches the transformation key. It ensures:
    - Non-matching tables are not transformed
    - Data remains unchanged when transformations don't apply
    - Selective transformation application works correctly

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup transformation that won't be applied (non-matching table)
    prefix = "Transformed-"

    # Define transformation function
    def transform_table(df: DataFrame, etl) -> DataFrame:
        df = df.withColumn("name", F.concat(F.lit(prefix), F.col("name")))
        return df

    # Configure ETL with transformation for a different table name
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["transformations"] = {
        "NotMatchingTable": transform_table  # Table name doesn't match actual source
    }

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # PHASE 2: Prepare test data (should remain unchanged)
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]
    expected_data = [r for r in init_data]  # No transformation expected

    # Set up bronze layer with original data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    # PHASE 3: Execute ingestion (transformation should not be applied)
    inserted_df = etl.ingest()
    silver_df = etl.read_silver_df()

    # Verify ingestion completed successfully
    assert bronze.df.count() == len(init_data)
    assert inserted_df is not None
    assert inserted_df.count() == len(init_data)
    assert silver_df.count() == len(init_data)

    # PHASE 4: Validate no transformation was applied (data unchanged)
    for i, row in enumerate(inserted_df.orderBy("id").collect()):
        assert row["name"] == expected_data[i].name  # Names should be unchanged


def test_ingest_include_columns_comparing(spark_: SparkSession) -> None:
    """
    Test selective column comparison using include_comparing_columns.

    This test verifies that only specified columns are used for change
    detection when include_comparing_columns is configured. It ensures:
    - Only included columns trigger change detection
    - Changes in excluded columns are ignored
    - Selective comparison logic works correctly
    - Performance optimization through reduced comparison scope

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup ETL with selective column comparison
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["include_comparing_columns"] = ["name"]  # Only compare 'name' column for changes

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Create initial data with both 'name' and 'ncol' columns
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1", ncol="Value-1"),
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="Value-2"),
        BronzeDataFrameRecord(id=3, name="Name-3", ncol="Value-3")
    ]
    expected_data = [r for r in init_data]

    # Set up bronze layer with additional column
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=[]
    )

    bronze.add_ncol_column() \
          .add_records(init_data) \
          .write() \
          .read()

    # 1. Initial ingestion baseline
    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify initial state with all columns present
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)
    assert NCOL in bronze.df.columns
    assert NCOL in inserted_df_1.columns
    assert NCOL in silver_df_1.columns

    # PHASE 2: Test selective change detection
    # use only name column for comparing
    new_data = [
        BronzeDataFrameRecord(id=11, name="Name-11", ncol="Value-11"),
    ]
    updated_data = [
        BronzeDataFrameRecord(id=1, name="Name-1-Updated", ncol="Value-1"),  # name changed - will be detected
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="Value-2-Updated")  # only ncol changed - ignored by include_comparing_columns
    ]
    expected_data += new_data
    expected_data += updated_data[:1]  # Only first update should be included (name change)

    # Add new records and update existing ones
    bronze.add_records(new_data) \
          .update_records(updated_data) \
          .write() \
          .read()

    # Sort expected data for comparison consistency
    expected_data = sorted(
        expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    # 2. Perform ingestion with selective comparison
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    # Verify selective comparison results
    assert bronze.df.count() == len(init_data) + len(new_data)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(new_data) + len(updated_data[:1])  # Only name change detected
    assert silver_df_2.count() == len(expected_data)

    # ID=1 should have 2 records (original + name update), ID=2 should have 1 (ncol change ignored)
    assert silver_df_2.filter(F.col("id") == 1).count() == 2
    assert silver_df_2.filter(F.col("id") == 2).count() == 1

    # PHASE 3: Validate selective comparison worked correctly
    for i, row in enumerate(silver_df_2.orderBy(F.col("id"), F.col("ROW_LOAD_DTS")).collect()):
        assert row["id"] == expected_data[i].id
        assert row["name"] == expected_data[i].name
        assert row["ncol"] == expected_data[i].ncol


def test_ingest_exclude_columns_comparing(spark_: SparkSession) -> None:
    """
    Test selective column comparison using exclude_comparing_columns.

    This test verifies that specified columns are excluded from change
    detection when exclude_comparing_columns is configured. It ensures:
    - Excluded columns don't trigger change detection
    - Changes in non-excluded columns are still detected
    - Exclusion logic works correctly with automatic exclusions
    - Proper handling of technical columns in comparison

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup ETL with column exclusion configuration
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["exclude_comparing_columns"] = [NCOL, "updated_at"]  # Exclude these columns from change detection

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Create initial data with both included and excluded columns
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1", ncol="Value-1"),
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="Value-2"),
        BronzeDataFrameRecord(id=3, name="Name-3", ncol="Value-3")
    ]
    expected_data = [r for r in init_data]

    # Set up bronze layer with additional column
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=[]
    )

    bronze.add_ncol_column() \
          .add_records(init_data) \
          .write() \
          .read()

    # 1. Initial ingestion baseline with excluded columns
    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify initial state with all columns present
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)
    assert NCOL in bronze.df.columns
    assert NCOL in inserted_df_1.columns
    assert NCOL in silver_df_1.columns

    # PHASE 2: Test selective change detection with column exclusion
    # use only name column for comparing
    new_data = [
        BronzeDataFrameRecord(id=11, name="Name-11", ncol="Value-11"),
    ]
    updated_data = [
        BronzeDataFrameRecord(id=1, name="Name-1-Updated", ncol="Value-1"),  # name changed - will be detected
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="Value-2-Updated")  # only ncol changed - ignored by exclude_comparing_columns
    ]
    expected_data += new_data
    expected_data += updated_data[:1]  # Only first update should be included (name change)

    # Add new records and update existing ones
    bronze.add_records(new_data) \
          .update_records(updated_data) \
          .write() \
          .read()

    # Sort expected data for comparison consistency
    expected_data = sorted(
        expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    # 2. Perform ingestion with column exclusion
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    # Verify column exclusion results
    assert bronze.df.count() == len(init_data) + len(new_data)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(new_data) + len(updated_data[:1])  # Only name change detected
    assert silver_df_2.count() == len(expected_data)

    # ID=1 should have 2 records (original + name update), ID=2 should have 1 (ncol change ignored)
    assert silver_df_2.filter(F.col("id") == 1).count() == 2
    assert silver_df_2.filter(F.col("id") == 2).count() == 1

    # PHASE 3: Validate column exclusion worked correctly
    for i, row in enumerate(silver_df_2.orderBy(F.col("id"), F.col("ROW_LOAD_DTS")).collect()):
        assert row["id"] == expected_data[i].id
        assert row["name"] == expected_data[i].name
        assert row["ncol"] == expected_data[i].ncol  # May be unchanged for excluded column updates


def test_ingest_delta_load(spark_: SparkSession) -> None:
    """
    Test delta load processing functionality.

    This test verifies that delta load mode works correctly when only
    changes are provided in the bronze layer. It ensures:
    - Delta loads are processed without full data refresh
    - Changed records are properly identified and processed
    - Historical data is preserved during delta processing
    - Broadcast join optimization works when enabled

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup delta load configuration and initial data
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["is_delta_load"] = True                # Enable delta load mode
    etl_kwargs["delta_load_use_broadcast"] = True     # Enable broadcast join optimization

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Create initial dataset for baseline
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2"),
        BronzeDataFrameRecord(id=3, name="Name-3"),
        BronzeDataFrameRecord(id=4, name="Name-4"),
        BronzeDataFrameRecord(id=5, name="Name-5")
    ]
    expected_data = [r for r in init_data]

    # Set up bronze layer with initial data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    # 1. Initial full load to establish baseline
    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify initial load completed successfully
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    # PHASE 2: Delta load simulation with changes only
    # Delta load
    new_data = [
        BronzeDataFrameRecord(id=6, name="Name-6"),
        BronzeDataFrameRecord(id=7, name="Name-7")
    ]
    updated_data = [
        BronzeDataFrameRecord(id=1, name="Name-1-Updated"),
        BronzeDataFrameRecord(id=2, name="Name-2-Updated")
    ]
    expected_data += new_data
    expected_data += updated_data

    # Clear existing data from bronze (simulating delta load where only changes are provided)
    bronze.delete_records([r.id for r in init_data]) \
          .write() \
          .read()

    # Add only the changed records to bronze (delta load pattern)
    bronze.add_records(new_data) \
          .add_records(updated_data) \
          .write() \
          .read()

    # Sort expected data for comparison consistency
    expected_data = sorted(
        expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    # 2. Perform delta load ingestion
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    # PHASE 3: Verify delta load results
    # Bronze should only contain changed records
    assert bronze.df.count() == len(new_data) + len(updated_data)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(new_data) + len(updated_data)  # All changes processed
    assert silver_df_2.count() == len(expected_data)  # All historical data preserved

    # Verify historization for updated records (should have 2 versions each)
    assert silver_df_2.filter(F.col("id") == 1).count() == 2  # Original + updated
    assert silver_df_2.filter(F.col("id") == 2).count() == 2  # Original + updated


def test_ingest_historize_false(spark_: SparkSession) -> None:
    """
    Test ingestion behavior when historization is disabled.

    This test verifies that when historization is turned off:
    - All records are processed on every run regardless of changes
    - No change detection logic is applied
    - Data is overwritten instead of historized
    - Performance optimization for non-historical loads

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup ETL with historization disabled
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["historize"] = False  # Disable historization for this test

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Create simple test data
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]

    # Set up bronze layer with test data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    # 1. First ingestion without historization
    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify first ingestion
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    # PHASE 2: Test repeated ingestion without historization
    # 2. Second ingestion with same data (should reprocess everything)
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    # Verify that all records are processed again (no change detection)
    assert bronze.df.count() == len(init_data)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(init_data)  # All records reprocessed
    assert silver_df_2.count() == len(init_data)    # No historical versions


def test_ingest_multiple_ids(spark_: SparkSession) -> None:
    """
    Test natural key handling with multiple columns.

    This test verifies that composite natural keys (multiple columns)
    are handled correctly. It ensures:
    - Multiple columns are properly concatenated to form NK
    - Concatenation string separator works correctly
    - Composite keys are unique and properly identify records
    - Multi-column natural key logic works in change detection

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup ETL with composite natural key configuration
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["nk_columns"] = ["id", NCOL]  # Use multiple columns for natural key
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Create test data with multiple columns for natural key
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1", ncol="id"),
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="id")
    ]

    # Set up bronze layer with composite key columns
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=[]
    )
    bronze.add_ncol_column() \
          .add_records(init_data) \
          .write() \
          .read()

    # 1. Perform ingestion with composite natural key
    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify ingestion completed successfully
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    # PHASE 2: Validate composite natural key generation
    for i, row in enumerate(silver_df_1.orderBy("id").collect()):
        assert row["id"] == init_data[i].id
        assert row["name"] == init_data[i].name
        assert row["ncol"] == init_data[i].ncol

        # Verify composite natural key is correctly concatenated
        assert row["NK"] == f"{init_data[i].id}{etl_kwargs['nk_column_concate_str']}{init_data[i].ncol}"


def test_ingest_custom_df_bronze(spark_: SparkSession) -> None:
    """
    Test ingestion using a custom bronze DataFrame instead of reading from storage.

    This test verifies that a pre-loaded DataFrame can be used as the bronze
    source instead of reading from the lakehouse. It ensures:
    - Custom DataFrames are accepted as bronze source
    - No file I/O occurs when custom DataFrame is provided
    - Data processing works identically with custom DataFrames
    - Flexibility for testing and alternative data sources

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup ETL with custom bronze DataFrame
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()

    # Create test data for custom DataFrame
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]

    # Generate bronze DataFrame without writing to storage
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    # Configure ETL to use custom DataFrame instead of reading from storage
    etl_kwargs["df_bronze"] = bronze.df  # Pass DataFrame directly
    etl.init(**etl_kwargs)

    # PHASE 2: Execute ingestion using custom DataFrame
    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify that no file I/O occurred (bronze data was not written to disk)
    with pytest.raises(Exception, match="[PATH_NOT_FOUND]"):
        # no written data, so custom df is used
        bronze.read()

    # Verify ingestion worked with custom DataFrame
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    # PHASE 3: Validate data correctness with custom DataFrame source
    for i, row in enumerate(silver_df_1.orderBy("id").collect()):
        assert row["id"] == init_data[i].id
        assert row["name"] == init_data[i].name


def test_ingest_partition_by_columns(spark_: SparkSession) -> None:
    """
    Test data partitioning functionality.

    This test verifies that data can be partitioned by specified columns
    to optimize query performance. It ensures:
    - Data is properly partitioned according to specified columns
    - Partition structure is correctly applied
    - Query performance optimization through partitioning
    - Proper handling of partition column selection

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup ETL with data partitioning configuration
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["partition_by_columns"] = ["id", "name"]  # Partition by multiple columns
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Create test data for partitioning
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]

    # Set up bronze layer with partitionable data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    # PHASE 2: Execute ingestion with partitioning
    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify ingestion completed successfully
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    # PHASE 3: Validate partitioning was applied correctly
    assert silver_df_1.rdd.getNumPartitions() == 2  # Each record should create its own partition


def test_ingest_with_constant_columns(spark_: SparkSession) -> None:
    """
    Test ingestion with constant columns added to the data.

    This test verifies that constant columns can be added to all records
    during ingestion. It ensures:
    - Constant columns are properly added to all records
    - Constant values are correctly applied
    - Constant columns can be part of natural key (part_of_nk=True)
    - Multiple constant columns work together
    - Different constant values create separate data lineages

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Setup ETL with constant columns configuration
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    # Define constant columns with one part of natural key
    constant_columns_europe = [
        ConstantColumn(name="instance", value="europe", part_of_nk=True),  # Part of NK
        ConstantColumn(name="data", value="value")                        # Not part of NK
    ]
    etl_kwargs["constant_columns"] = constant_columns_europe
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Create initial test data
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]
    expected_data = [r for r in init_data]

    # Set up bronze layer with base data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    # 1. Initial ingestion with constant columns
    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify ingestion with constant columns
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    # Verify constant columns were added correctly
    for row in silver_df_1.collect():
        assert row["INSTANCE"] == "europe"  # Constant column with specified value
        assert row["DATA"] == "value"       # Second constant column

    # PHASE 2: Test different constant values creating separate data lineages
    # 1. Change instance
    constant_columns_asia = [
        ConstantColumn(name="instance", value="asia", part_of_nk=True),  # Different value for NK
        ConstantColumn(name="data", value="value")
    ]
    etl_kwargs["constant_columns"] = constant_columns_asia
    etl.init(**etl_kwargs)

    # Same data with different constant values should create new records
    expected_data += init_data

    # 2. Ingest same data with different constant values
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    # Verify different constant values create separate records
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == 2      # Same data, different instance
    assert silver_df_2.count() == 4        # Total: europe (2) + asia (2)

    # PHASE 3: Add new data to specific instance
    # 2. Add new data to asia instance

    etl_kwargs["constant_columns"] = constant_columns_asia
    etl.init(**etl_kwargs)

    # Add completely new records to the asia instance
    new_data = [
        BronzeDataFrameRecord(id=3, name="Name-3"),
        BronzeDataFrameRecord(id=4, name="Name-4")
    ]
    expected_data += new_data

    bronze.add_records(new_data) \
          .write() \
          .read()

    # 3. Ingest new data for asia instance
    inserted_df_3 = etl.ingest()
    silver_df_3 = etl.read_silver_df()

    # Verify new data was added to correct instance
    assert inserted_df_3 is not None
    assert inserted_df_3.count() == len(new_data)      # Only new records
    assert silver_df_3.count() == len(expected_data)   # All records across instances

    # PHASE 4: Validate data distribution across constant value instances
    assert silver_df_3.filter(F.col("INSTANCE") == "europe").count() == 2  # Original europe data
    assert silver_df_3.filter(F.col("INSTANCE") == "asia").count() == 4    # Asia data: original (2) + new (2)


def test_ingest_mlv_code(spark_: SparkSession) -> None:
    """
    Test Materialized Lake View (MLV) code generation.

    This test verifies that the ETL service can generate proper MLV SQL code
    for creating materialized views of the historized data. It ensures:
    - MLV code is generated correctly with all columns
    - Schema changes are reflected in MLV code updates
    - Column order and structure are maintained
    - MLV naming convention follows expected patterns
    - Generated SQL is syntactically correct and complete

    Args:
        spark_: SparkSession fixture for testing
    """
    # PHASE 1: Initial setup and baseline MLV code generation
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    # Expected MLV code for initial schema (without additional columns)
    expected_mlv_code = """
SELECT
`PK`,
`NK`,
`id`,
`name`,
`created_at`,
`updated_at`,
`ROW_IS_CURRENT`,
`ROW_HIST_NUMBER`,
`ROW_UPDATE_DTS`,
`ROW_DELETE_DTS`,
`ROW_LOAD_DTS`
"""

    # Create initial test data
    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]

    # Set up bronze layer with initial data
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    # 1. Perform initial ingestion to generate baseline MLV code
    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    # Verify initial ingestion and MLV code generation
    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    # Validate MLV properties and initial code structure
    assert etl.mlv_name == f"{etl._dest_table.table_path}{etl._mlv_suffix}"
    assert expected_mlv_code in etl.mlv_code

    # PHASE 2: Schema evolution and MLV code update
    # 1. Add columns (schema change)
    # Expected MLV code after schema evolution with additional columns
    expected_mlv_code = """
SELECT
`PK`,
`NK`,
`id`,
`name`,
`created_at`,
`updated_at`,
`ncol`,
`INSTANCE`,
`DATA`,
`ROW_IS_CURRENT`,
`ROW_HIST_NUMBER`,
`ROW_UPDATE_DTS`,
`ROW_DELETE_DTS`,
`ROW_LOAD_DTS`
FROM cte_mlv
"""
    # Add constant columns for schema evolution test
    constant_columns = [
        ConstantColumn(name="instance", value="europe", part_of_nk=True),
        ConstantColumn(name="data", value="value")
    ]
    etl_kwargs["constant_columns"] = constant_columns
    etl.init(**etl_kwargs)

    # Add new data with additional column
    new_data = [
        BronzeDataFrameRecord(id=11, name="Name-11", ncol="Value-11"),
        BronzeDataFrameRecord(id=12, name="Name-12", ncol="Value-12")
    ]
    bronze.add_ncol_column() \
          .add_records(new_data) \
          .write() \
          .read()

    # Track new columns that should appear in MLV code
    new_columns = ["INSTANCE", "DATA", NCOL]

    # 2. Perform second ingestion with schema changes
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    # PHASE 3: Validate schema evolution in MLV code
    # Verify new columns are present in all layers
    assert NCOL in bronze.df.columns
    assert NCOL in inserted_df_2.columns
    assert NCOL in silver_df_2.columns
    assert all(col in silver_df_2.columns for col in new_columns)
    assert all(col in inserted_df_2.columns for col in new_columns)

    # Validate that MLV code was updated to include all new columns
    assert expected_mlv_code in etl.mlv_code
