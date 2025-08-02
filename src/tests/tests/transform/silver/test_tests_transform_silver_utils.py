import os

from datetime import datetime
from pyspark.sql import SparkSession, types as T, functions as F
from fabricengineer.transform.lakehouse import LakehouseTable
from fabricengineer.transform.silver.utils import get_mock_table_path
from tests.transform.silver.utils import BronzeDataFrameRecord, BronzeDataFrameDataGenerator


default_bronze_table = LakehouseTable(
    lakehouse="BronzeLakehouse",
    schema="default_schema",
    table="default_table"
)


def generate_init_data(count: int = 10) -> list[BronzeDataFrameRecord]:
    """Generate a list of BronzeDataFrameRecord with sequential IDs and names."""
    return [
        BronzeDataFrameRecord(
            id=i,
            name=f"Name-{i}"
        ) for i in range(1, count + 1)
    ]


def test_bronze_dataframe_record_default_values():
    """Test that BronzeDataFrameRecord creates with default values"""
    record = BronzeDataFrameRecord(id=1, name="test")

    assert record.id == 1
    assert record.name == "test"
    assert isinstance(record.created_at, datetime)
    assert isinstance(record.updated_at, datetime)


def test_bronze_dataframe_record_custom_values():
    """Test that BronzeDataFrameRecord creates with custom values"""
    record = BronzeDataFrameRecord(
        id=42,
        name="custom_name",
        created_at=datetime.strptime("2024-01-01", "%Y-%m-%d"),
        updated_at=datetime.strptime("2024-12-31", "%Y-%m-%d")
    )

    assert record.id == 42
    assert record.name == "custom_name"
    assert record.created_at == datetime.strptime("2024-01-01", "%Y-%m-%d")
    assert record.updated_at == datetime.strptime("2024-12-31", "%Y-%m-%d")


def test_init_with_default_record_count(spark_: SparkSession):
    """Test initialization with default record count"""
    count = 10
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)

    generator.write().read()

    assert generator.spark == spark_
    assert generator.table == default_bronze_table
    assert generator.init_data == init_data
    assert generator.df is not None
    assert generator.df.count() == 10

    for i, row in enumerate(generator.df.orderBy(F.col("id").asc()).collect()):
        assert row["id"] == init_data[i].id
        assert row["name"] == init_data[i].name
        assert row["created_at"] == init_data[i].created_at
        assert row["updated_at"] == init_data[i].updated_at
        assert row["created_at"] is not None
        assert row["updated_at"] is not None


def test_generate_df_structure(spark_: SparkSession):
    """Test that generated DataFrame has correct structure"""
    count = 3
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)
    df = generator.df

    # Check schema
    expected_fields = ["id", "name", "created_at", "updated_at"]
    actual_fields = df.columns
    assert all(field in actual_fields for field in expected_fields)

    # Check data types
    schema_dict = {field.name: field.dataType for field in df.schema.fields}
    assert isinstance(schema_dict["id"], T.IntegerType)
    assert isinstance(schema_dict["name"], T.StringType)
    assert isinstance(schema_dict["created_at"], T.TimestampType)
    assert isinstance(schema_dict["updated_at"], T.TimestampType)


def test_generate_df_content(spark_: SparkSession):
    """Test that generated DataFrame has correct content"""
    count = 3
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)

    df = generator.df

    rows = df.collect()
    assert len(rows) == 3

    # Check first row
    first_row = rows[0]
    assert first_row["id"] == 1
    assert first_row["name"] == "Name-1"
    assert first_row["created_at"] is not None
    assert first_row["updated_at"] is not None


def test_write_method(spark_: SparkSession):
    """Test write method functionality"""
    count = 2
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)
    result = generator.write()

    # Check that method returns  for chaining
    assert result == generator
    assert os.path.exists(get_mock_table_path(default_bronze_table))


def test_read_method(spark_: SparkSession):
    """Test read method functionality"""
    count = 2
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)

    result = generator.write()

    # Check that method returns  for chaining
    assert result == generator

    # Test reading the DataFrame
    generator.read()
    assert generator.df is not None
    assert generator.df.count() == 2


def test_add_records(spark_: SparkSession):
    """Test adding records to the generator"""
    count = 2
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)
    initial_count = generator.df.count()

    new_records = [
        BronzeDataFrameRecord(id=100, name="NewRecord1"),
        BronzeDataFrameRecord(id=101, name="NewRecord2")
    ]

    generator.add_records(new_records).write().read()

    # Check that records were added
    assert generator.df.count() == initial_count + len(new_records)

    # Check that new records exist
    new_rows = generator.df.filter(F.col("id").isin([100, 101])).collect()
    assert len(new_rows) == 2

    names = [row["name"] for row in new_rows]
    assert "NewRecord1" in names
    assert "NewRecord2" in names


def test_update_records(spark_: SparkSession):
    """Test updating records in the generator"""
    count = 5
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)

    # Update record with id=2
    update_records = [
        BronzeDataFrameRecord(id=2, name="UpdatedName")
    ]

    generator.update_records(update_records).write().read()

    # Check that record was updated
    updated_row = generator.df.filter(F.col("id") == 2).collect()[0]
    assert updated_row["name"] == "UpdatedName"
    assert updated_row["created_at"] != updated_row["updated_at"]  # created_at should remain the same, updated_at should change

    # Check that other records weren't affected
    other_rows = generator.df.filter(F.col("id") != 2).collect()
    for row in other_rows:
        assert "UpdatedName" not in row["name"]


def test_delete_records(spark_: SparkSession):
    """Test deleting records from the generator"""
    count = 5
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)
    initial_count = generator.df.count()

    # Delete records with ids 2 and 4
    ids_to_delete = [2, 4]
    generator.delete_records(ids_to_delete).write().read()

    # Check that records were deleted
    assert generator.df.count() == initial_count - 2

    # Check that deleted records don't exist
    remaining_ids = [row["id"] for row in generator.df.collect()]
    assert 2 not in remaining_ids
    assert 4 not in remaining_ids

    # Check that other records still exist
    assert 1 in remaining_ids
    assert 3 in remaining_ids
    assert 5 in remaining_ids


def test_empty_operations(spark_: SparkSession, ):
    """Test operations with empty lists"""
    count = 2
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)
    initial_count = generator.df.count()

    # Test empty operations
    generator.add_records([])
    generator.update_records([])
    generator.delete_records([])

    generator.write().read()

    # Count should remain the same
    assert generator.df.count() == initial_count


def test_add_ncol_column(spark_: SparkSession):
    """Test adding ncol column to the DataFrame"""
    count = 2
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)
    generator.write().read()

    assert "ncol" not in generator.df.columns

    generator.add_ncol_column().write().read()

    assert "ncol" in generator.df.columns

    # Check that ncol is None for all records
    ncol_values = generator.df.select("ncol").collect()
    assert all(row["ncol"] is None for row in ncol_values)


def test_remove_ncol_column(spark_: SparkSession):
    """Test removing ncol column from the DataFrame"""
    count = 2
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)
    generator.add_ncol_column().write().read()

    assert "ncol" in generator.df.columns

    generator.remove_ncol_column().write().read()

    assert "ncol" not in generator.df.columns


def test_add_records_with_ncol(spark_: SparkSession):
    """Test adding records with ncol column"""
    count = 2
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)
    generator.add_ncol_column().write().read()

    new_records = [
        BronzeDataFrameRecord(id=100, name="NewRecord1", ncol="Value1"),
        BronzeDataFrameRecord(id=101, name="NewRecord2", ncol="Value2")
    ]

    generator.add_records(new_records).write().read()

    # Check that new records were added with ncol values
    new_rows = generator.df.filter(F.col("id").isin([100, 101])).collect()
    assert len(new_rows) == 2

    for row in new_rows:
        assert row["ncol"] is not None
        assert row["ncol"].startswith("Value")


def test_add_records_with_ncol_and_remove_ncol(spark_: SparkSession):
    """Test adding records with ncol and then removing ncol column"""
    count = 2
    init_data = generate_init_data(count)
    generator = BronzeDataFrameDataGenerator(spark_, default_bronze_table, init_data=init_data)
    generator.add_ncol_column().write().read()

    new_records = [
        BronzeDataFrameRecord(id=100, name="NewRecord1", ncol="Value1"),
        BronzeDataFrameRecord(id=101, name="NewRecord2", ncol="Value2")
    ]

    generator.add_records(new_records).write().read()

    new_rows = generator.df.filter(F.col("id").isin([100, 101])).collect()
    assert len(new_rows) == 2

    generator.remove_ncol_column().write().read()

    # Check that ncol column was removed
    assert "ncol" not in generator.df.columns

    # Check that new records still exist without ncol
    new_rows = generator.df.filter(F.col("id").isin([100, 101])).collect()
    assert len(new_rows) == 2
    for row in new_rows:
        assert "ncol" not in row
