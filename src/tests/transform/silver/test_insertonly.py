import pytest
from uuid import uuid4
from pyspark.sql import SparkSession
from fabricengineer.transform.silver.insertonly import (
    SilverIngestionInsertOnlyService
)
from fabricengineer.transform.lakehouse import LakehouseTable


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
    assert etl._exclude_comparing_columns == set(["id", "PK", "NK", "ROW_DELETE_DTS", "ROW_LOAD_DTS"] + etl_kwargs["exclude_comparing_columns"])
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

    # mit kommentar beschreiben und dann generieren lassen <3
