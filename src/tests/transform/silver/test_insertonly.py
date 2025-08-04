import pytest

from datetime import datetime
from uuid import uuid4
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F
)

from tests.transform.silver.utils import BronzeDataFrameRecord, BronzeDataFrameDataGenerator
from fabricengineer.transform.silver.utils import ConstantColumn
from fabricengineer.transform.silver.insertonly import (
    SilverIngestionInsertOnlyService
)
from fabricengineer.transform.lakehouse import LakehouseTable


# pytest src/tests/transform/silver -v

NCOL = "ncol"


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
    "nk_column_concate_str": "_",
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
    assert etl._dw_columns[3] == etl._row_load_dts_column


def test_init_etl_fail_params(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()

    # df_bronze should be DataFrame
    with pytest.raises(TypeError, match=f"should be type of {DataFrame.__name__}"):
        kwargs = etl_kwargs.copy() | {"df_bronze": "str"}
        etl.init(**kwargs)

    # spark_ should be SparkSession
    with pytest.raises(TypeError, match=f"should be type of {SparkSession.__name__}"):
        kwargs = etl_kwargs.copy() | {"spark_": "str"}
        etl.init(**kwargs)

    # historize should be bool
    with pytest.raises(TypeError, match=f"should be type of {bool.__name__}"):
        kwargs = etl_kwargs.copy() | {"historize": "str"}
        etl.init(**kwargs)

    # create_historized_mlv should be bool
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


def test_ingest(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    prefix = "Name-"
    init_count = 10
    init_data = [
        BronzeDataFrameRecord(id=i, name=f"{prefix}{i}")
        for i in range(1, init_count + 1)
    ]
    current_expected_data = [r for r in init_data]
    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data,
        init_name_prefix=prefix
    )

    bronze.write().read()

    for i, row in enumerate(bronze.df.orderBy("id").collect()):
        assert row["name"] == init_data[i].name

    # 1. Init silver ingestion
    inserted_df = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    assert inserted_df is not None
    assert inserted_df.count() == len(current_expected_data)
    assert bronze.df.count() == len(current_expected_data)
    assert silver_df_1.count() == len(current_expected_data)
    assert all(True for column in bronze.df.columns if column in inserted_df.columns)
    assert all(True for column in etl._dw_columns if column in inserted_df.columns)
    assert all(True for column in bronze.df.columns if column in silver_df_1.columns)
    assert all(True for column in etl._dw_columns if column in silver_df_1.columns)

    for i, row in enumerate(inserted_df.orderBy("id").collect()):
        assert row["name"] == init_data[i].name
        assert row["created_at"] == init_data[i].created_at
        assert row["updated_at"] == init_data[i].updated_at

    for i, row in enumerate(silver_df_1.orderBy("id").collect()):
        assert row["name"] == current_expected_data[i].name
        assert row["created_at"] == current_expected_data[i].created_at
        assert row["updated_at"] == current_expected_data[i].updated_at

    # 2. Ingest without any changes
    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    assert inserted_df_2 is not None
    assert inserted_df_2.count() == 0
    assert silver_df_2.count() == len(current_expected_data)
    assert all(True for column in bronze.df.columns if column in inserted_df.columns)
    assert all(True for column in etl._dw_columns if column in inserted_df.columns)

    # 3. Ingest with changes (inserts, updates, deletes)
    new_data = [
        BronzeDataFrameRecord(id=100, name="Name-100"),
        BronzeDataFrameRecord(id=101, name="Name-101"),
        BronzeDataFrameRecord(id=102, name="Name-102"),
        BronzeDataFrameRecord(id=103, name="Name-103"),
        BronzeDataFrameRecord(id=104, name="Name-104")
    ]
    current_expected_data += new_data

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

    bronze.add_records(new_data) \
          .update_records(updated_data) \
          .delete_records(deleted_data_ids) \
          .write() \
          .read()

    inserted_df_3 = etl.ingest()
    silver_df_3 = etl.read_silver_df()

    changed_count = len(new_data) + len(updated_data) + len(deleted_data_ids)

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
        print(f"Row {i}: {row}")
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


def test_ingest_new_added_column(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]
    expected_data = [r for r in init_data]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    inserted_df_1 = etl.ingest()
    silver_df = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df.count() == len(init_data)
    assert NCOL not in bronze.df.columns
    assert NCOL not in inserted_df_1.columns
    assert NCOL not in silver_df.columns

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
    bronze.add_ncol_column() \
          .add_records(new_data) \
          .write() \
          .read()

    expected_data = sorted(
        expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    assert bronze.df.count() == 4
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == 2
    assert silver_df_2.count() == 4
    assert NCOL in bronze.df.columns
    assert NCOL in inserted_df_2.columns
    assert NCOL in silver_df_2.columns

    for i, row in enumerate(inserted_df_2.orderBy(F.col("id").asc(), F.col("ROW_LOAD_DTS").asc()).collect()):
        assert row["name"] == new_data[i].name
        assert row["ncol"] == new_data[i].ncol


def test_ingest_remove_column(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1", ncol="Value-1"),
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="Value-2")
    ]
    expected_data = [r for r in init_data]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=[]
    )
    bronze.add_ncol_column() \
          .add_records(init_data) \
          .write() \
          .read()

    inserted_df_1 = etl.ingest()
    silver_df = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df.count() == len(init_data)
    assert NCOL in bronze.df.columns
    assert NCOL in inserted_df_1.columns
    assert NCOL in silver_df.columns

    for i, row in enumerate(inserted_df_1.orderBy(F.col("id").asc(), F.col("ROW_LOAD_DTS").asc()).collect()):
        assert row["id"] == init_data[i].id
        assert row["name"] == init_data[i].name
        assert row["ncol"] == init_data[i].ncol
        assert row["ncol"] is not None

    # 1. Remove column
    bronze.remove_ncol_column()
    new_data = [
        BronzeDataFrameRecord(id=11, name="Name-11"),
        BronzeDataFrameRecord(id=12, name="Name-12")
    ]
    expected_data += new_data

    updated_data = [
        BronzeDataFrameRecord(id=1, name="Name-Updated")
    ]
    expected_data += updated_data

    bronze.add_records(new_data) \
          .update_records(updated_data) \
          .write() \
          .read()

    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    expected_data = sorted(
        expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    assert bronze.df.count() == len(init_data) + len(new_data)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(new_data) + len(updated_data)
    assert silver_df_2.count() == len(expected_data)
    assert NCOL not in bronze.df.columns
    assert NCOL in inserted_df_2.columns
    assert NCOL in silver_df_2.columns

    for i, row in enumerate(silver_df_2.orderBy(F.col("id").asc(), F.col("ROW_LOAD_DTS").asc()).collect()):
        assert row["id"] == expected_data[i].id
        assert row["name"] == expected_data[i].name
        assert row["ncol"] == expected_data[i].ncol


def test_ingest_with_transformations(spark_: SparkSession):
    prefix = "Transformed-"

    def transform_table(df: DataFrame, etl) -> DataFrame:
        df = df.withColumn("name", F.concat(F.lit(prefix), F.col("name")))
        return df

    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["transformations"] = {
        etl_kwargs["source_table"].table: transform_table
    }

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]
    expected_data = [
        BronzeDataFrameRecord(id=r.id, name=f"{prefix}{r.name}")
        for r in
        init_data
    ]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    inserted_df = etl.ingest()
    silver_df = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df is not None
    assert inserted_df.count() == len(init_data)
    assert silver_df.count() == len(init_data)

    for i, row in enumerate(inserted_df.orderBy("id").collect()):
        assert row["name"] == expected_data[i].name


def test_ingest_with_transformation_star(spark_: SparkSession):
    prefix = "Transformed-"

    def transform_table(df: DataFrame, etl) -> DataFrame:
        df = df.withColumn("name", F.concat(F.lit(prefix), F.col("name")))
        return df

    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["transformations"] = {
        "*": transform_table
    }

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]
    expected_data = [
        BronzeDataFrameRecord(id=r.id, name=f"{prefix}{r.name}")
        for r in
        init_data
    ]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    inserted_df = etl.ingest()
    silver_df = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df is not None
    assert inserted_df.count() == len(init_data)
    assert silver_df.count() == len(init_data)

    for i, row in enumerate(inserted_df.orderBy("id").collect()):
        assert row["name"] == expected_data[i].name


def test_ingest_with_transformation_not_applied(spark_: SparkSession):
    prefix = "Transformed-"

    def transform_table(df: DataFrame, etl) -> DataFrame:
        df = df.withColumn("name", F.concat(F.lit(prefix), F.col("name")))
        return df

    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["transformations"] = {
        "NotMatchingTable": transform_table
    }

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]
    expected_data = [r for r in init_data]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    inserted_df = etl.ingest()
    silver_df = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df is not None
    assert inserted_df.count() == len(init_data)
    assert silver_df.count() == len(init_data)

    for i, row in enumerate(inserted_df.orderBy("id").collect()):
        assert row["name"] == expected_data[i].name


def test_ingest_include_columns_comparing(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["include_comparing_columns"] = ["name"]

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1", ncol="Value-1"),
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="Value-2"),
        BronzeDataFrameRecord(id=3, name="Name-3", ncol="Value-3")
    ]
    expected_data = [r for r in init_data]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=[]
    )

    bronze.add_ncol_column() \
          .add_records(init_data) \
          .write() \
          .read()

    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)
    assert NCOL in bronze.df.columns
    assert NCOL in inserted_df_1.columns
    assert NCOL in silver_df_1.columns

    # use only name column for comparing
    new_data = [
        BronzeDataFrameRecord(id=11, name="Name-11", ncol="Value-11"),
    ]
    updated_data = [
        BronzeDataFrameRecord(id=1, name="Name-1-Updated", ncol="Value-1"),
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="Value-2-Updated")  # ignored by include_comparing_columns
    ]
    expected_data += new_data
    expected_data += updated_data[:1]

    bronze.add_records(new_data) \
          .update_records(updated_data) \
          .write() \
          .read()
    expected_data = sorted(
        expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data) + len(new_data)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(new_data) + len(updated_data[:1])
    assert silver_df_2.count() == len(expected_data)

    assert silver_df_2.filter(F.col("id") == 1).count() == 2
    assert silver_df_2.filter(F.col("id") == 2).count() == 1

    for i, row in enumerate(silver_df_2.orderBy(F.col("id"), F.col("ROW_LOAD_DTS")).collect()):
        assert row["id"] == expected_data[i].id
        assert row["name"] == expected_data[i].name
        assert row["ncol"] == expected_data[i].ncol


def test_ingest_exclude_columns_comparing(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["exclude_comparing_columns"] = [NCOL, "updated_at"]

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1", ncol="Value-1"),
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="Value-2"),
        BronzeDataFrameRecord(id=3, name="Name-3", ncol="Value-3")
    ]
    expected_data = [r for r in init_data]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=[]
    )

    bronze.add_ncol_column() \
          .add_records(init_data) \
          .write() \
          .read()

    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)
    assert NCOL in bronze.df.columns
    assert NCOL in inserted_df_1.columns
    assert NCOL in silver_df_1.columns

    # use only name column for comparing
    new_data = [
        BronzeDataFrameRecord(id=11, name="Name-11", ncol="Value-11"),
    ]
    updated_data = [
        BronzeDataFrameRecord(id=1, name="Name-1-Updated", ncol="Value-1"),
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="Value-2-Updated")  # ignored by exclude_comparing_columns
    ]
    expected_data += new_data
    expected_data += updated_data[:1]

    bronze.add_records(new_data) \
          .update_records(updated_data) \
          .write() \
          .read()
    expected_data = sorted(
        expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data) + len(new_data)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(new_data) + len(updated_data[:1])
    assert silver_df_2.count() == len(expected_data)

    assert silver_df_2.filter(F.col("id") == 1).count() == 2
    assert silver_df_2.filter(F.col("id") == 2).count() == 1

    for i, row in enumerate(silver_df_2.orderBy(F.col("id"), F.col("ROW_LOAD_DTS")).collect()):
        assert row["id"] == expected_data[i].id
        assert row["name"] == expected_data[i].name
        assert row["ncol"] == expected_data[i].ncol


def test_ingest_delta_load(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["is_delta_load"] = True
    etl_kwargs["delta_load_use_broadcast"] = True

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2"),
        BronzeDataFrameRecord(id=3, name="Name-3"),
        BronzeDataFrameRecord(id=4, name="Name-4"),
        BronzeDataFrameRecord(id=5, name="Name-5")
    ]
    expected_data = [r for r in init_data]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

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

    bronze.delete_records([r.id for r in init_data]) \
          .write() \
          .read()

    bronze.add_records(new_data) \
          .add_records(updated_data) \
          .write() \
          .read()

    expected_data = sorted(
        expected_data,
        key=lambda r: (r.id, r.created_at)
    )

    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    assert bronze.df.count() == len(new_data) + len(updated_data)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(new_data) + len(updated_data)
    assert silver_df_2.count() == len(expected_data)
    assert silver_df_2.filter(F.col("id") == 1).count() == 2
    assert silver_df_2.filter(F.col("id") == 2).count() == 2


def test_ingest_historize_false(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["historize"] = False

    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_2 is not None
    assert inserted_df_2.count() == len(init_data)
    assert silver_df_2.count() == len(init_data)


def test_ingest_multiple_ids(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["nk_columns"] = ["id", NCOL]
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1", ncol="id"),
        BronzeDataFrameRecord(id=2, name="Name-2", ncol="id")
    ]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=[]
    )
    bronze.add_ncol_column() \
          .add_records(init_data) \
          .write() \
          .read()

    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    for i, row in enumerate(silver_df_1.orderBy("id").collect()):
        assert row["id"] == init_data[i].id
        assert row["name"] == init_data[i].name
        assert row["ncol"] == init_data[i].ncol

        assert row["NK"] == f"{init_data[i].id}{etl_kwargs['nk_column_concate_str']}{init_data[i].ncol}"


def test_ingest_custom_df_bronze(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    etl_kwargs["df_bronze"] = bronze.df
    etl.init(**etl_kwargs)

    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    with pytest.raises(Exception, match="[PATH_NOT_FOUND]"):
        # no written data, so custom df is used
        bronze.read()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    for i, row in enumerate(silver_df_1.orderBy("id").collect()):
        assert row["id"] == init_data[i].id
        assert row["name"] == init_data[i].name


def test_ingest_partition_by_columns(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl_kwargs["partition_by_columns"] = ["id", "name"]
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)
    assert silver_df_1.rdd.getNumPartitions() == 2


def test_ingest_with_constant_columns(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    constant_columns_europe = [
        ConstantColumn(name="instance", value="europe", part_of_nk=True),
        ConstantColumn(name="data", value="value")
    ]
    etl_kwargs["constant_columns"] = constant_columns_europe
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]
    expected_data = [r for r in init_data]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    for row in silver_df_1.collect():
        assert row["INSTANCE"] == "europe"
        assert row["DATA"] == "value"

    # 1. Change instance
    constant_columns_asia = [
        ConstantColumn(name="instance", value="asia", part_of_nk=True),
        ConstantColumn(name="data", value="value")
    ]
    etl_kwargs["constant_columns"] = constant_columns_asia
    etl.init(**etl_kwargs)

    expected_data += init_data

    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    assert inserted_df_2 is not None
    assert inserted_df_2.count() == 2
    assert silver_df_2.count() == 4

    # 2. Add new data to asia instance

    etl_kwargs["constant_columns"] = constant_columns_asia
    etl.init(**etl_kwargs)

    new_data = [
        BronzeDataFrameRecord(id=3, name="Name-3"),
        BronzeDataFrameRecord(id=4, name="Name-4")
    ]
    expected_data += new_data

    bronze.add_records(new_data) \
          .write() \
          .read()

    inserted_df_3 = etl.ingest()
    silver_df_3 = etl.read_silver_df()

    assert inserted_df_3 is not None
    assert inserted_df_3.count() == len(new_data)
    assert silver_df_3.count() == len(expected_data)

    assert silver_df_3.filter(F.col("INSTANCE") == "europe").count() == 2
    assert silver_df_3.filter(F.col("INSTANCE") == "asia").count() == 4


def test_ingest_with_mlv_values(spark_: SparkSession):
    etl_kwargs = get_default_etl_kwargs(spark_=spark_)
    etl = SilverIngestionInsertOnlyService()
    etl.init(**etl_kwargs)

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

    init_data = [
        BronzeDataFrameRecord(id=1, name="Name-1"),
        BronzeDataFrameRecord(id=2, name="Name-2")
    ]

    bronze = BronzeDataFrameDataGenerator(
        spark=spark_,
        table=etl_kwargs["source_table"],
        init_data=init_data
    )

    bronze.write().read()

    inserted_df_1 = etl.ingest()
    silver_df_1 = etl.read_silver_df()

    assert bronze.df.count() == len(init_data)
    assert inserted_df_1 is not None
    assert inserted_df_1.count() == len(init_data)
    assert silver_df_1.count() == len(init_data)

    assert etl.mlv_name == f"{etl._dest_table.table_path}{etl._mlv_suffix}"
    assert expected_mlv_code in etl.mlv_code

    # 1. Add columns (schema change)
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
    constant_columns = [
        ConstantColumn(name="instance", value="europe", part_of_nk=True),
        ConstantColumn(name="data", value="value")
    ]
    etl_kwargs["constant_columns"] = constant_columns
    etl.init(**etl_kwargs)

    new_data = [
        BronzeDataFrameRecord(id=11, name="Name-11", ncol="Value-11"),
        BronzeDataFrameRecord(id=12, name="Name-12", ncol="Value-12")
    ]
    bronze.add_ncol_column() \
          .add_records(new_data) \
          .write() \
          .read()

    new_columns = ["INSTANCE", "DATA", NCOL]

    inserted_df_2 = etl.ingest()
    silver_df_2 = etl.read_silver_df()

    assert NCOL in bronze.df.columns
    assert NCOL in inserted_df_2.columns
    assert NCOL in silver_df_2.columns
    assert all(col in silver_df_2.columns for col in new_columns)
    assert all(col in inserted_df_2.columns for col in new_columns)
    assert expected_mlv_code in etl.mlv_code
