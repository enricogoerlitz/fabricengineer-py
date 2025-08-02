import pytest

from uuid import uuid4
from pyspark.sql import SparkSession
from tests.transform.silver.utils import BronzeDataFrameRecord, BronzeDataFrameDataGenerator
from fabricengineer.transform.silver.utils import ConstantColumn
from fabricengineer.transform.silver.insertonly import (
    SilverIngestionInsertOnlyService
)
from fabricengineer.transform.lakehouse import LakehouseTable


# pytest src/tests/transform/silver -v


default_etl_kwargs = {
    "spark_": None,
    "source_table": None,
    "destination_table": None,
    "nk_columns": ["id"],
    "constant_columns": [],
    "is_delta_load": False,
    "delta_load_use_broadcast": True,
    "transformations": {},
    "exclude_comparing_columns": [],
    "include_comparing_columns": [],
    "historize": True,
    "partition_by_columns": [],
    "df_bronze": None,
    "create_historized_mlv": True,
    "is_testing_mock": True
}


def get_default_etl_kwargs(spark_: SparkSession) -> dict:
    source_table = LakehouseTable(
        lakehouse="BronzeLakehouse",
        schema="default_schema",
        table=str(uuid4())
    )
    dest_table = LakehouseTable(
        lakehouse="SilverLakehouse",
        schema=source_table.schema,
        table=source_table.table
    )
    kwargs = default_etl_kwargs.copy()
    kwargs["spark_"] = spark_
    kwargs["source_table"] = source_table
    kwargs["destination_table"] = dest_table
    return kwargs


def test_init_etl(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["constant_columns"] = [
        ConstantColumn(name="instance", value="VTSD", part_of_nk=True),
        ConstantColumn(name="other", value="column")
    ]
    etl = SilverIngestionInsertOnlyService()

    etl.init(**etl_kwargs)

    assert etl._is_initialized is True
    assert etl._spark == spark_
    assert etl._src_table == etl_kwargs["source_table"]
    assert etl._dest_table == etl_kwargs["destination_table"]
    assert etl._nk_columns == etl_kwargs["nk_columns"]
    assert etl._constant_columns == etl_kwargs["constant_columns"]
    assert etl._is_delta_load == etl_kwargs["is_delta_load"]
    assert etl._delta_load_use_broadcast == etl_kwargs["delta_load_use_broadcast"]
    assert etl._transformations == etl_kwargs["transformations"]
    assert etl._exclude_comparing_columns == set(["id", "PK", "NK", "ROW_DELETE_DTS", "ROW_LOAD_DTS", "OTHER", "INSTANCE"] + etl_kwargs["exclude_comparing_columns"])
    assert etl._include_comparing_columns == etl_kwargs["include_comparing_columns"]
    assert etl._historize == etl_kwargs["historize"]
    assert etl._partition_by == etl_kwargs["partition_by_columns"]
    assert etl._df_bronze is None
    assert etl._is_create_hist_mlv == etl_kwargs["create_historized_mlv"]
    assert etl._is_testing_mock == etl_kwargs["is_testing_mock"]

    assert etl.mlv_code is None
    assert etl.mlv_name == f"{etl._dest_table.lakehouse}.{etl._dest_table.schema}.{etl._dest_table.table}_h"

    assert len(etl._dw_columns) == 4
    assert etl._dw_columns[0] == etl._pk_column_name
    assert etl._dw_columns[1] == etl._nk_column_name
    assert etl._dw_columns[2] == etl._row_delete_dts_column
    assert etl._dw_columns[3] == etl._ldts_column


def test_init_etl_fail_params(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()

    # df_bronze should be DataFrame
    with pytest.raises(TypeError, match="should be type of"):
        kwargs = etl_kwargs.copy() | {"df_bronze": "str"}
        etl.init(**kwargs)

    # spark_ should be SparkSession
    with pytest.raises(TypeError, match="should be type of"):
        kwargs = etl_kwargs.copy() | {"spark_": "str"}
        etl.init(**kwargs)

    # TODO: weitere Parameter prüfen
    # mit kommentar beschreiben und dann generieren lassen <3


def test_ingest_general(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    current_expected_count = 10
    prefix = "Name-"
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_record_count=current_expected_count,
        init_name_prefix=prefix
    )

    bronze.write().read()

    for i, row in enumerate(bronze.df.orderBy("id").collect(), 1):
        assert row["name"] == f"{prefix}{i}"

    inserted_df = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    assert inserted_df is not None
    assert inserted_df.count() == current_expected_count
    assert all(True for column in bronze.df.columns if column in inserted_df.columns)
    assert all(True for column in etl._dw_columns if column in inserted_df.columns)
    assert all(True for column in bronze.df.columns if column in silver_df_1.columns)
    assert all(True for column in etl._dw_columns if column in silver_df_1.columns)
    assert bronze.df.count() == current_expected_count
    assert silver_df_1.count() == current_expected_count

    for i, row in enumerate(inserted_df.orderBy("id").collect(), 1):
        assert row["name"] == f"{prefix}{i}"

    for i, row in enumerate(silver_df_1.orderBy("id").collect(), 1):
        assert row["name"] == f"{prefix}{i}"

    inserted_df_2 = etl.ingest()

    assert inserted_df_2 is not None
    assert inserted_df_2.count() == 0
    assert silver_df_1.count() == current_expected_count
    assert all(True for column in bronze.df.columns if column in inserted_df.columns)
    assert all(True for column in etl._dw_columns if column in inserted_df.columns)

    new_data = [
        BronzeDataFrameRecord(id=100, name="Name-100"),
        BronzeDataFrameRecord(id=101, name="Name-101"),
        BronzeDataFrameRecord(id=102, name="Name-102")
    ]

    updated_data = [
        BronzeDataFrameRecord(id=4, name="Name-1-Updated"),
        BronzeDataFrameRecord(id=5, name="Name-2-Updated"),
        BronzeDataFrameRecord(id=6, name="Name-3-Updated")
    ]

    deleted_data = [1, 7, 9]

    bronze.add_records(new_data) \
          .update_records(updated_data) \
          .delete_records(deleted_data) \
          .write() \
          .read()

    inserted_df_3 = etl.ingest()

    assert inserted_df_3 is not None
    # assert inserted_df_3.count() == len(new_data) + len(update_data) + len(delete_data)
