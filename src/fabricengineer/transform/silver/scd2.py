import os

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from fabricengineer.transform.silver.utils import (
    ConstantColumn,
    generate_uuid,
    get_mock_table_path
)
from fabricengineer.transform.lakehouse import LakehouseTable
from fabricengineer.transform.silver.base import BaseSilverIngestionServiceImpl


class SilverIngestionSCD2Service(BaseSilverIngestionServiceImpl):
    _is_initialized: bool = False

    def init(
        self,
        *,
        spark_: SparkSession,
        source_table: LakehouseTable,
        destination_table: LakehouseTable,
        nk_columns: list[str],
        constant_columns: list[ConstantColumn],
        is_delta_load: bool,
        delta_load_use_broadcast: bool,
        transformations: dict,
        exclude_comparing_columns: list[str] | None = None,
        include_comparing_columns: list[str] | None = None,
        historize: bool = True,
        partition_by_columns: list[str] = None,
        df_bronze: DataFrame = None,

        pk_column_name: str = "PK",
        nk_column_name: str = "NK",
        nk_column_concate_str: str = "_",
        row_is_current_column: str = "ROW_IS_CURRENT",
        row_update_dts_column: str = "ROW_UPDATE_DTS",
        row_delete_dts_column: str = "ROW_DELETE_DTS",
        row_load_dts_column: str = "ROW_LOAD_DTS",

        is_testing_mock: bool = False
    ) -> None:
        dw_columns = [
            pk_column_name,
            nk_column_name,
            row_is_current_column,
            row_update_dts_column,
            row_delete_dts_column,
            row_load_dts_column
        ]

        super().init(
            spark_=spark_,
            source_table=source_table,
            destination_table=destination_table,
            nk_columns=nk_columns,
            constant_columns=constant_columns,
            is_delta_load=is_delta_load,
            delta_load_use_broadcast=delta_load_use_broadcast,
            transformations=transformations,
            exclude_comparing_columns=exclude_comparing_columns or [],
            include_comparing_columns=include_comparing_columns or [],
            historize=historize,
            partition_by_columns=partition_by_columns or [],
            df_bronze=df_bronze,
            dw_columns=dw_columns,

            pk_column_name=pk_column_name,
            nk_column_name=nk_column_name,
            nk_column_concate_str=nk_column_concate_str,
            row_is_current_column=row_is_current_column,
            row_update_dts_column=row_update_dts_column,
            row_delete_dts_column=row_delete_dts_column,
            row_load_dts_column=row_load_dts_column,

            is_testing_mock=is_testing_mock
        )

        self._validate_scd2_params()

        self._is_initialized = True

    def __str__(self) -> str:
        return super().__str__()

    def _validate_scd2_params(self) -> None:
        pass

    def ingest(self) -> None:
        if not self._is_initialized:
            raise RuntimeError("The SilverIngestionInsertOnlyService is not initialized. Call the init method first.")

        self._current_timestamp = datetime.now()
        df_bronze, df_silver = self._generate_dataframes()

        # 1.
        target_columns_ordered = self._get_columns_ordered(df_bronze, last_columns=[
            self._row_load_dts_column,
            self._row_update_dts_column,
            self._row_delete_dts_column,
            self._row_is_current_column
        ])

        do_overwrite = (
            df_silver is None or
            (
                not self._historize and
                not self._is_delta_load
                # If we are not historizing but performing a delta load,
                # we need to update the silver-layer data.
                # We should not overwrite the silver-layer data,
                # because the delta load (bronze layer) do not contain all the data!
            )
        )
        if do_overwrite:
            df_inital_load = df_bronze.select(target_columns_ordered)
            self._write_df(df_inital_load, "overwrite")
            return

        # 2.
        columns_to_compare = self._get_columns_to_compare(df_bronze)

        join_condition = (df_bronze[self._nk_column_name] == df_silver[self._nk_column_name])
        df_joined = df_bronze.join(df_silver, join_condition, "outer")

        # 3.
        _, neq_condition = self._compare_condition(df_bronze, df_silver, columns_to_compare)
        updated_filter_condition = self._updated_filter(df_bronze, df_silver, neq_condition)

        df_new_records = self._filter_new_records(df_joined, df_bronze, df_silver)
        df_updated_records = self._filter_updated_records(df_joined, df_bronze, updated_filter_condition)

        # 4.
        df_new_data = df_new_records.unionByName(df_updated_records) \
                                    .select(target_columns_ordered) \
                                    .dropDuplicates(["PK"])

        self._write_df(df_new_data, "append")

        # 5.
        df_expired_records = self._filter_expired_records(df_joined, df_silver, updated_filter_condition)

        # 6.
        if self._is_delta_load:
            self._exec_merge_into(df_expired_records, "df_expired_records")
            return

        df_deleted_records = self._filter_deleted_records(df_joined, df_bronze, df_silver)

        df_merge_into_records = df_expired_records.unionByName(df_deleted_records) \
                                                  .select(target_columns_ordered)
        self._exec_merge_into(df_merge_into_records, "df_merge_into_records")

    def _generate_dataframes(self) -> tuple[DataFrame, DataFrame]:
        df_bronze = self._create_bronze_df()
        df_bronze = self._apply_transformations(df_bronze)

        df_silver = self._create_silver_df()

        if df_silver is None:
            return df_bronze, df_silver

        df_bronze = self._add_missing_columns(df_bronze, df_silver)
        df_silver = self._add_missing_columns(df_silver, df_bronze)

        if self._is_delta_load and self._delta_load_use_broadcast:
            df_bronze = F.broadcast(df_bronze)

        return df_bronze, df_silver

    def _updated_filter(self, df_bronze: DataFrame, df_silver: DataFrame, neq_condition):
        updated_filter = (
            (df_bronze[self._nk_column_name].isNotNull()) &
            (df_silver[self._nk_column_name].isNotNull()) &
            (df_silver[self._row_is_current_column] == 1) &
            (neq_condition)
        )

        return updated_filter

    def _filter_new_records(self, df_joined: DataFrame, df_bronze: DataFrame, df_silver: DataFrame) -> DataFrame:
        new_records_filter = (df_silver[self._nk_column_name].isNull())
        df_new_records = df_joined.filter(new_records_filter) \
                                  .select(df_bronze["*"])

        return df_new_records

    def _filter_updated_records(self, df_joined: DataFrame, df_bronze: DataFrame, updated_filter) -> DataFrame:
        # Select not matching bronze columns
        df_updated_records = df_joined.filter(updated_filter) \
                                      .select(df_bronze["*"])

        return df_updated_records

    def _filter_expired_records(self, df_joined: DataFrame, df_silver: DataFrame, updated_filter) -> DataFrame:
        # Select not matching silver columns
        df_expired_records = df_joined.filter(updated_filter) \
                                      .select(df_silver["*"]) \
                                      .withColumn(self._row_update_dts_column, F.lit(self._current_timestamp)) \
                                      .withColumn(self._row_delete_dts_column, F.lit(None).cast("timestamp")) \
                                      .withColumn(self._row_is_current_column, F.lit(0))

        return df_expired_records

    def _filter_deleted_records(self, df_joined: DataFrame, df_bronze: DataFrame, df_silver: DataFrame) -> DataFrame:
        df_deleted_records = df_joined.filter(df_bronze[self._nk_column_name].isNull()) \
                                      .select(df_silver["*"]) \
                                      .withColumn(self._row_update_dts_column, F.lit(self._current_timestamp)) \
                                      .withColumn(self._row_delete_dts_column, F.lit(self._current_timestamp)) \
                                      .withColumn(self._row_is_current_column, F.lit(0))

        return df_deleted_records

    def _create_bronze_df(self) -> DataFrame:
        sql_select_source = f"SELECT * FROM {self._src_table.table_path}"
        if isinstance(self._df_bronze, DataFrame):
            df = self._df_bronze
        elif not self._is_testing_mock:
            df = self._spark.sql(sql_select_source)
        else:
            df = self._spark.read.format("parquet").load(get_mock_table_path(self._src_table))

        self._validate_nk_columns_in_df(df)

        for constant_column in self._constant_columns:
            if constant_column.name not in df.columns:
                df = df.withColumn(constant_column.name, F.lit(constant_column.value))

        df = df.withColumn(self._pk_column_name, generate_uuid())  \
               .withColumn(self._nk_column_name, F.concat_ws(self._nk_column_concate_str, *self._nk_columns)) \
               .withColumn(self._row_update_dts_column, F.lit(None).cast("timestamp")) \
               .withColumn(self._row_delete_dts_column, F.lit(None).cast("timestamp")) \
               .withColumn(self._row_is_current_column, F.lit(1)) \
               .withColumn(self._row_load_dts_column, F.lit(self._current_timestamp))

        return df

    def _create_silver_df(self) -> DataFrame:
        if self._is_testing_mock:
            if not os.path.exists(get_mock_table_path(self._dest_table)):
                return None
        elif not self._spark.catalog.tableExists(self._dest_table.table_path):
            return None

        df = self.read_silver_df()

        self._validate_nk_columns_in_df(df)

        for constant_column in self._constant_columns:
            if constant_column.name not in df.columns:
                df = df.withColumn(constant_column.name, F.lit(None))

            if constant_column.part_of_nk:
                df = df.filter(F.col(constant_column.name) == constant_column.value)

        df = df.withColumn(self._nk_column_name, F.concat_ws(self._nk_column_concate_str, *self._nk_columns))

        return df

    def _exec_merge_into(self, df_source: DataFrame, view_name: str) -> None:
        # Zielpfad oder Tabellennamen auflösen
        if self._is_testing_mock:
            target_path = get_mock_table_path(self._dest_table)
            delta_table = DeltaTable.forPath(self._spark, target_path)
        else:
            table_name = self._dest_table.table_path
            delta_table = DeltaTable.forName(self._spark, table_name)

        # Merge-Logik ausführen
        delta_table.alias("target") \
            .merge(
                df_source.alias("source"),
                f"target.{self._pk_column_name} = source.{self._pk_column_name}"
            ) \
            .whenMatchedUpdate(set={
                self._row_update_dts_column: f"source.{self._row_update_dts_column}",
                self._row_delete_dts_column: f"source.{self._row_delete_dts_column}",
                self._row_is_current_column: f"source.{self._row_is_current_column}"
            }) \
            .execute()


etl = SilverIngestionSCD2Service()
