# flake8: noqa

from typing import Callable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from fabricengineer.transform.lakehouse import LakehouseTable
from fabricengineer.transform.silver.utils import ConstantColumn
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
        row_hist_number_column: str = "ROW_HIST_NUMBER",
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
            row_hist_number_column=row_hist_number_column,
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

    def ingest(self):
        # 1.
        df_bronze, df_silver = self._generate_dataframes()

        target_columns_ordered = self._get_columns_ordered(df_bronze)

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
        _, neq_condition = self._get_compare_condition(df_bronze, df_silver, columns_to_compare)
        updated_filter_condition = self._get_updated_filter(df_bronze, df_silver, neq_condition)

        df_new_records = self._filter_new_records(df_joined, df_bronze, df_silver)
        df_updated_records = self._filter_updated_records(df_joined, df_bronze, updated_filter_condition)

        # 4.
        df_new_data = df_new_records.unionByName(df_updated_records).select(target_columns_ordered).dropDuplicates(["PK"])
        self._write_df(df_new_data, "append")

        # 5.
        df_expired_records = self._filter_expired_records(df_joined, df_silver, updated_filter_condition)
        self._exec_merge_into(df_expired_records, "df_expired_records")

        # 6.
        if self._is_delta_load:
            return

        df_deleted_records = self._filter_deleted_records(df_joined, df_bronze, df_silver)
        self._exec_merge_into(df_deleted_records, "df_deleted_records")

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
    
    def _get_compare_condition(self, df_bronze: DataFrame, df_silver: DataFrame, columns_to_compare: list[str]):
        eq_condition = (
            (df_bronze[columns_to_compare[0]] == df_silver[columns_to_compare[0]]) |
            (df_bronze[columns_to_compare[0]].isNull() & df_silver[columns_to_compare[0]].isNull())
        )

        if len(columns_to_compare) == 1:
            return eq_condition, ~eq_condition
        

        for compare_column in columns_to_compare[1:]:
            eq_condition &= (
                (df_bronze[compare_column] == df_silver[compare_column]) |
                (df_bronze[compare_column].isNull() & df_silver[compare_column].isNull())
            )
        
        return eq_condition, ~eq_condition

    def _get_updated_filter(self, df_bronze: DataFrame, df_silver: DataFrame, neq_condition):
        updated_filter = (
            (df_bronze[self._nk_column].isNotNull()) &
            (df_silver[self._nk_column].isNotNull()) &
            (df_silver[self._row_is_current_column] == 1) &
            (neq_condition)
        )

        return updated_filter

    def _filter_new_records(self, df_joined: DataFrame, df_bronze: DataFrame, df_silver: DataFrame) -> DataFrame:
        new_records_filter = (df_silver[self._nk_column].isNull())
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
                                      .withColumn(self._row_update_dts_column, self._current_timestamp) \
                                      .withColumn(self._row_delete_dts_column, F.lit(None).cast("timestamp")) \
                                      .withColumn(self._row_is_current_column, F.lit(0))
       
        return df_expired_records

    def _filter_deleted_records(self, df_joined: DataFrame, df_bronze: DataFrame, df_silver: DataFrame) -> DataFrame:
        df_deleted_records = df_joined.filter(df_bronze[self._nk_column].isNull()) \
                                      .select(df_silver["*"]) \
                                      .withColumn(self._row_update_dts_column, self._current_timestamp) \
                                      .withColumn(self._row_delete_dts_column, self._current_timestamp) \
                                      .withColumn(self._row_is_current_column, F.lit(0))

        return df_deleted_records

    def _create_bronze_df(self) -> DataFrame:
        sql_select_source = f"SELECT * FROM {self._sql_src_table}"
        df = self._df_bronze if self._df_bronze is not None else self._spark.sql(sql_select_source)

        self._validate_nk_columns_in_df(df)

        for constant_column in self._constant_columns:
            if constant_column.name not in df.columns:
                df = df.withColumn(constant_column.name, F.lit(constant_column.value))

        df = df.withColumn(self._pk_column, generate_uuid())  \
               .withColumn(self._nk_column, F.concat_ws(self._nk_column_concate_str, *self._nk_columns)) \
               .withColumn(self._row_update_dts_column, F.lit(None).cast("timestamp")) \
               .withColumn(self._row_delete_dts_column, F.lit(None).cast("timestamp")) \
               .withColumn(self._row_is_current_column, F.lit(1)) \
               .withColumn(self._ldts_column, F.lit(self._current_timestamp))

        return df

    def _create_silver_df(self) -> DataFrame:
        if not self._spark.catalog.tableExists(self._sql_dist_table):
            return None

        sql_select_destination = f"SELECT * FROM {self._sql_dist_table}"
        df = self._spark.sql(sql_select_destination)

        self._validate_nk_columns_in_df(df)

        for constant_column in self._constant_columns:
            if constant_column.name not in df.columns:
                df = df.withColumn(constant_column.name, F.lit(None))

            if constant_column.part_of_nk:
                df = df.filter(F.col(constant_column.name) == constant_column.value)

        df = df.withColumn(self._nk_column, F.concat_ws(self._nk_column_concate_str, *self._nk_columns))

        return df

    def _add_missing_columns(self, df_target: DataFrame, df_source: DataFrame) -> DataFrame:
        missing_columns = [missing_column for missing_column in df_source.columns if missing_column not in df_target.columns]

        for missing_column in missing_columns:
            df_target = df_target.withColumn(missing_column, F.lit(None))

        return df_target

    def _get_columns_to_compare(self, df: DataFrame) -> list[str]:
        if isinstance(self._include_comparing_columns, list) and len(self._include_comparing_columns) >= 1:
            self._validate_include_comparing_columns(df)
            return self._include_comparing_columns

        comparison_columns = [column for column in df.columns if column not in self._exclude_comparing_columns]

        return comparison_columns

    def _get_columns_ordered(self, df: DataFrame) -> list[str]:
        all_columns = [
            column
            for column in df.columns
            if column not in self._dw_columns
        ]

        return [self._pk_column, self._nk_column] + all_columns + [
            self._ldts_column,
            self._row_update_dts_column,
            self._row_delete_dts_column,
            self._row_is_current_column
        ]

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        transform_fn: Callable = self._transformations.get(self._src_tablename)
        transform_fn_all: Callable = self._transformations.get("*")

        if transform_fn_all is not None:
            df = transform_fn_all(df, self)

        if transform_fn is None:
            return df
        
        return transform_fn(df, self)

    def _exec_merge_into(self, df_source: DataFrame, view_name: str) -> None:
        df_source.createOrReplaceTempView(view_name)
        self._spark.sql(f"""
            MERGE INTO {self._sql_dist_table} AS target
            USING {view_name} AS source
            ON target.{self._pk_column} = source.{self._pk_column}
            WHEN MATCHED THEN
            UPDATE SET
                {self._row_update_dts_column} = source.{self._row_update_dts_column},
                {self._row_delete_dts_column} = source.{self._row_delete_dts_column},
                {self._row_is_current_column} = source.{self._row_is_current_column}
        """)

    def _write_df(self, df: DataFrame, write_mode: str) -> None:
        df.write \
            .format("delta") \
            .mode(write_mode) \
            .option("mergeSchema", "true") \
            .partitionBy(*self._partition_by) \
            .saveAsTable(self._sql_dist_table)


etl = SilverIngestionService()

print("MASTER NOTEBOOK PASSES 'etl':", str(etl))