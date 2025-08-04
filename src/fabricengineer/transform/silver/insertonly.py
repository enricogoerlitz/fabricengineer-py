import os

from datetime import datetime
from typing import Callable
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
    Window
)

from fabricengineer.transform.silver.utils import (
    LakehouseTable,
    ConstantColumn,
    generate_uuid,
    get_mock_table_path
)
from fabricengineer.transform.silver.base import BaseSilverIngestionService


class SilverIngestionInsertOnlyService(BaseSilverIngestionService):
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
        create_historized_mlv: bool = True,
        mlv_suffix: str = "_h",

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
        self._mlv_code = None

        self._is_testing_mock = is_testing_mock
        self._spark = spark_
        self._df_bronze = df_bronze
        self._historize = historize
        self._is_create_hist_mlv = create_historized_mlv
        self._mlv_suffix = mlv_suffix
        self._is_delta_load = is_delta_load
        self._delta_load_use_broadcast = delta_load_use_broadcast
        self._src_table = source_table
        self._dest_table = destination_table
        self._nk_columns = nk_columns
        self._include_comparing_columns = include_comparing_columns

        self._exclude_comparing_columns = exclude_comparing_columns or []
        self._transformations: dict[str, Callable] = transformations or {}
        self._constant_columns: list[ConstantColumn] = constant_columns or []
        self._partition_by: list[str] = partition_by_columns or []

        self._pk_column_name = pk_column_name
        self._nk_column_name = nk_column_name
        self._nk_column_concate_str = nk_column_concate_str
        self._row_hist_number_column = row_hist_number_column
        self._row_is_current_column = row_is_current_column
        self._row_update_dts_column = row_update_dts_column
        self._row_delete_dts_column = row_delete_dts_column
        self._row_load_dts_column = row_load_dts_column

        self._validate_parameters()
        self._set_spark_config()

        self._dw_columns = [
            self._pk_column_name,
            self._nk_column_name,
            self._row_delete_dts_column,
            self._row_load_dts_column
        ]

        self._exclude_comparing_columns = set(
            [self._pk_column_name]
            + self._nk_columns
            + self._dw_columns
            + self._exclude_comparing_columns
            + [column.name for column in self._constant_columns]
        )

        self._spark.catalog.clearCache()
        self._is_initialized = True

    @property
    def mlv_name(self) -> str:
        return f"{self._dest_table.table_path}{self._mlv_suffix}"

    @property
    def mlv_code(self) -> str:
        return self._mlv_code

    def __str__(self) -> str:
        if not self._is_initialized:
            return super.__str__(self)

        return str({
            "historize": self._historize,
            "is_delta_load": self._is_delta_load,
            "delta_load_use_broadcast": self._delta_load_use_broadcast,
            "src_table_path": self._src_table.table_path,
            "dist_table_path": self._dest_table.table_path,
            "nk_columns": self._nk_columns,
            "include_comparing_columns": self._include_comparing_columns,
            "exclude_comparing_columns": self._exclude_comparing_columns,
            "transformations": self._transformations,
            "constant_columns": self._constant_columns,
            "partition_by": self._partition_by,
            "pk_column": self._pk_column_name,
            "nk_column": self._nk_column_name,
            "nk_column_concate_str": self._nk_column_concate_str,
            "row_update_dts_column": self._row_update_dts_column,
            "row_delete_dts_column": self._row_delete_dts_column,
            "ldts_column": self._row_load_dts_column,
            "dw_columns": self._dw_columns
        })

    def _validate_parameters(self) -> None:
        """Validates the in constructor setted parameters, so the etl can run.

        Raises:
            ValueError: when a valueerror occurs
            TypeError: when a typerror occurs
            Exception: generic exception
        """

        if self._df_bronze is not None:
            self._validate_param_isinstance(self._df_bronze, "df_bronze", DataFrame)

        self._validate_param_isinstance(self._spark, "spark", SparkSession)
        self._validate_param_isinstance(self._historize, "historize", bool)
        self._validate_param_isinstance(self._is_create_hist_mlv, "create_historized_mlv", bool)
        self._validate_param_isinstance(self._is_delta_load, "is_delta_load", bool)
        self._validate_param_isinstance(self._delta_load_use_broadcast, "delta_load_use_broadcast", bool)
        self._validate_param_isinstance(self._transformations, "transformations", dict)
        self._validate_param_isinstance(self._src_table, "src_table", LakehouseTable)
        self._validate_param_isinstance(self._dest_table, "dest_table", LakehouseTable)
        self._validate_param_isinstance(self._include_comparing_columns, "include_columns_from_comparing", list)
        self._validate_param_isinstance(self._exclude_comparing_columns, "exclude_columns_from_comparing", list)
        self._validate_param_isinstance(self._partition_by, "partition_by_columns", list)
        self._validate_param_isinstance(self._pk_column_name, "pk_column", str)
        self._validate_param_isinstance(self._nk_column_name, "nk_column", str)
        self._validate_param_isinstance(self._nk_columns, "nk_columns", list)
        self._validate_param_isinstance(self._nk_column_concate_str, "nk_column_concate_str", str)
        self._validate_param_isinstance(self._mlv_suffix, "mlv_suffix", str)
        self._validate_param_isinstance(self._constant_columns, "constant_columns", list)
        self._validate_param_isinstance(self._row_load_dts_column, "row_load_dts_column", str)
        self._validate_param_isinstance(self._row_hist_number_column, "row_hist_number_column", str)
        self._validate_param_isinstance(self._row_is_current_column, "row_is_current_column", str)
        self._validate_param_isinstance(self._row_update_dts_column, "row_update_dts_column", str)
        self._validate_param_isinstance(self._row_delete_dts_column, "row_delete_dts_column", str)

        self._validate_min_length(self._pk_column_name, "pk_column", 2)
        self._validate_min_length(self._nk_column_name, "nk_column", 2)
        self._validate_min_length(self._src_table.lakehouse, "src_lakehouse", 3)
        self._validate_min_length(self._src_table.schema, "src_schema", 1)
        self._validate_min_length(self._src_table.table, "src_tablename", 3)
        self._validate_min_length(self._dest_table.lakehouse, "dest_lakehouse", 3)
        self._validate_min_length(self._dest_table.schema, "dest_schema", 1)
        self._validate_min_length(self._dest_table.table, "dest_tablename", 3)
        self._validate_min_length(self._nk_columns, "nk_columns", 1)
        self._validate_min_length(self._nk_column_concate_str, "nk_column_concate_str", 1)
        self._validate_min_length(self._mlv_suffix, "mlv_suffix", 1)
        self._validate_min_length(self._row_load_dts_column, "row_load_dts_column", 3)
        self._validate_min_length(self._row_hist_number_column, "row_hist_number_column", 3)
        self._validate_min_length(self._row_is_current_column, "row_is_current_column", 3)
        self._validate_min_length(self._row_update_dts_column, "row_update_dts_column", 3)
        self._validate_min_length(self._row_delete_dts_column, "row_delete_dts_column", 3)

        for key, fn in self._transformations.items():
            print("Transformation function:", fn)
            if not callable(fn):
                err_msg = f"The transformation function for key '{key}' is not callable."
                raise TypeError(err_msg)

        for constant_column in self._constant_columns:
            self._validate_param_isinstance(constant_column, "constant_column", ConstantColumn)

    def _validate_param_isinstance(self, param, param_name: str, obj_class) -> None:
        """Validates a parameter to be the expected class instance

        Args:
            param (any): parameter
            param_name (str): parametername
            obj_class (_type_): class

        Raises:
            TypeError: when actual type is different from expected type
        """
        if not isinstance(param, obj_class):
            err_msg = f"The param '{param_name}' should be type of {obj_class.__name__}, but was {str(param.__class__)}"
            raise TypeError(err_msg)

    def _validate_min_length(self, param, param_name: str, min_length: int) -> None:
        """Validates a string or list to be not none and has a minimum length

        Args:
            param (_type_): parameter
            param_name (str): parametername
            min_length (int): minimum lenght

        Raises:
            TypeError: when actual type is different from expected type
            ValueError: when parametervalue is to short
        """
        if not isinstance(param, str) and not isinstance(param, list):
            err_msg = f"The param '{param_name}' should be type of string or list, but was {str(param.__class__)}"
            raise TypeError(err_msg)

        param_length = len(param)
        if param_length < min_length:
            err_msg = f"Param length to short. The minimum length of the param '{param_name}' is {min_length} but was {param_length}"
            raise ValueError(err_msg)

    def _validate_constant_columns(self) -> None:
        """Validates the given constant columns to be an instance of ConstantColumns and
        list contains only one part_of_nk=True, because of the following filtering of the dataframe.

        It should have just one part_of_nk=True, because the dataframe will filtered later by the
        constant_column.name, if part_of_nk=True.
        If part_of_nk=True should be supported more then once, then we need to implement
        an "and" filtering.

        Raises:
            TypeError: when an item of the list is not an instance of ConstantColumn
            ValueError: when list contains more then one ConstantColumn with part_of_nk=True
        """
        nk_count = 0
        for column in self._constant_columns:
            if column.part_of_nk:
                nk_count += 1

            if not isinstance(column, ConstantColumn):
                err_msg = "Invalid items in constant_columns found. All items should be instance of ConstantColumn"
                raise TypeError(err_msg)

            if nk_count > 1:
                err_msg = "In constant_columns are more then one part_of_nk=True, what is not supported!"
                raise ValueError(err_msg)

    def _validate_nk_columns_in_df(self, df: DataFrame) -> None:
        """Validates the given dataframe. The given dataframe should contain all natural key columns,
        because all natural key columns will selected and used for concatitation.

        Args:
            df (DataFrame): dataframe to validate

        Raises:
            ValueError: when dataframe does not contain all natural key columns
        """
        df_columns = set(df.columns)
        for column in self._nk_columns:
            if column in df_columns:
                continue

            err_msg = f"The NK Column '{column}' does not exist in df columns: {df_columns}"
            raise ValueError(err_msg)

    def _validate_include_comparing_columns(self, df: DataFrame) -> None:
        self._validate_param_isinstance(self._include_comparing_columns, "include_comparing_columns", list)

        if len(self._include_comparing_columns) == 0:
            err_msg = "The param 'include_comparing_columns' is present, but don't contains any columns."
            raise ValueError(err_msg)

        for include_column in self._include_comparing_columns:
            if include_column in df.columns:
                continue

            err_msg = f"The column '{include_column}' should be compared, but is not given in df."
            raise ValueError(err_msg)

    def _validate_partition_by_columns(self, df: DataFrame) -> None:
        self._validate_param_isinstance(self._partition_by, "partition_by", list)

        for partition_column in self._partition_by:
            if partition_column in df.columns:
                continue

            err_msg = f"The column '{partition_column}' should be partitioned, but is not given in df."
            raise ValueError(err_msg)

    def _set_spark_config(self) -> None:
        """Sets additional spark configurations

        spark.sql.parquet.vorder.enabled: Setting "spark.sql.parquet.vorder.enabled" to "true" in PySpark config enables a feature called vectorized parquet decoding.
                                                  This optimizes the performance of reading Parquet files by leveraging vectorized instructions and processing multiple values at once, enhancing overall processing speed.

        Setting "spark.sql.parquet.int96RebaseModeInRead" and "spark.sql.legacy.parquet.int96RebaseModeInWrite" to "CORRECTED" ensures that Int96 values (a specific timestamp representation used in Parquet files) are correctly rebased during both reading and writing operations.
        This is crucial for maintaining consistency and accuracy, especially when dealing with timestamp data across different systems or time zones.
        Similarly, configuring "spark.sql.parquet.datetimeRebaseModeInRead" and "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" to "CORRECTED" ensures correct handling of datetime values during Parquet file operations.
        By specifying this rebasing mode, potential discrepancies or errors related to datetime representations are mitigated, resulting in more reliable data processing and analysis workflows.
        """
        self._spark.conf.set("spark.sql.parquet.vorder.enabled", "true")

        self._spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        self._spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        self._spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        self._spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    def ingest(self) -> DataFrame:
        if not self._is_initialized:
            raise RuntimeError("The SilverIngestionInsertOnlyService is not initialized. Call the init method first.")

        self._current_timestamp = datetime.now()

        # 1.
        df_bronze, df_silver, has_schema_changed = self._generate_dataframes()

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
            self._create_or_replace_historized_mlv(has_schema_changed, target_columns_ordered)
            return df_inital_load

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
        df_data_to_insert = df_new_records.unionByName(df_updated_records).select(target_columns_ordered).dropDuplicates(["PK"])

        # 6.
        if self._is_delta_load:
            self._write_df(df_data_to_insert, "append")
            self._create_or_replace_historized_mlv(has_schema_changed, target_columns_ordered)
            return df_data_to_insert

        df_deleted_records = self._filter_deleted_records(df_joined, df_bronze, df_silver).select(target_columns_ordered)
        df_data_to_insert = df_data_to_insert.unionByName(df_deleted_records)
        self._write_df(df_data_to_insert, "append")
        self._create_or_replace_historized_mlv(has_schema_changed, target_columns_ordered)
        return df_data_to_insert

    def read_silver_df(self) -> DataFrame:
        if self._is_testing_mock:
            if not os.path.exists(get_mock_table_path(self._dest_table)):
                return None
        elif not self._spark.catalog.tableExists(self._dest_table.table_path):
            return None

        sql_select_destination = f"SELECT * FROM {self._dest_table.table_path}"
        df = self._spark.sql(sql_select_destination) if not self._is_testing_mock else self._spark.read.format("delta").load(get_mock_table_path(self._dest_table))
        return df

    def _generate_dataframes(self) -> tuple[DataFrame, DataFrame, bool]:
        df_bronze = self._create_bronze_df()
        df_bronze = self._apply_transformations(df_bronze)

        df_silver = self._create_silver_df()

        has_schema_changed = self._has_schema_change(df_bronze, df_silver)

        if df_silver is None:
            return df_bronze, df_silver, has_schema_changed

        df_bronze = self._add_missing_columns(df_bronze, df_silver)
        df_silver = self._add_missing_columns(df_silver, df_bronze)

        if self._is_delta_load and self._delta_load_use_broadcast:
            df_bronze = F.broadcast(df_bronze)

        return df_bronze, df_silver, has_schema_changed

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
            (df_bronze[self._nk_column_name].isNotNull()) &
            (df_silver[self._nk_column_name].isNotNull()) &
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
                                      .withColumn(self._row_delete_dts_column, F.lit(None).cast("timestamp"))

        return df_expired_records

    def _filter_deleted_records(self, df_joined: DataFrame, df_bronze: DataFrame, df_silver: DataFrame) -> DataFrame:
        df_deleted_records = df_joined.filter((df_bronze[self._nk_column_name].isNull()) & (df_silver[self._row_delete_dts_column].isNull())) \
                                      .select(df_silver["*"]) \
                                      .withColumn(self._pk_column_name, generate_uuid()) \
                                      .withColumn(self._row_delete_dts_column, F.lit(self._current_timestamp)) \
                                      .withColumn(self._row_load_dts_column, F.lit(self._current_timestamp))

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
               .withColumn(self._row_delete_dts_column, F.lit(None).cast("timestamp")) \
               .withColumn(self._row_load_dts_column, F.lit(self._current_timestamp))

        return df

    def _create_silver_df(self) -> DataFrame:
        if self._is_testing_mock:
            if not os.path.exists(get_mock_table_path(self._dest_table)):
                return None
        elif not self._spark.catalog.tableExists(self._dest_table.table_path):
            return None

        sql_select_destination = f"SELECT * FROM {self._dest_table.table_path}"
        df = self._spark.sql(sql_select_destination) if not self._is_testing_mock else self._spark.read.format("parquet").load(get_mock_table_path(self._dest_table))

        self._validate_nk_columns_in_df(df)

        for constant_column in self._constant_columns:
            if constant_column.name not in df.columns:
                df = df.withColumn(constant_column.name, F.lit(None))

            if constant_column.part_of_nk:
                df = df.filter(F.col(constant_column.name) == constant_column.value)

        df = df.withColumn(self._nk_column_name, F.concat_ws(self._nk_column_concate_str, *self._nk_columns))

        return_columns = df.columns

        window_spec = Window.partitionBy(self._nk_columns).orderBy(df[self._row_load_dts_column].desc())
        df_with_rownum = df.withColumn("ROW_NUMBER", F.row_number().over(window_spec))
        df = df_with_rownum.filter(df_with_rownum["ROW_NUMBER"] == 1).select(return_columns)

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
        all_columns = [column for column in df.columns if column not in self._dw_columns]

        return [self._pk_column_name, self._nk_column_name] + all_columns + [
            self._row_load_dts_column,
            self._row_delete_dts_column
        ]

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        transform_fn: Callable = self._transformations.get(self._src_table.table)
        transform_fn_all: Callable = self._transformations.get("*")

        if transform_fn_all is not None:
            df = transform_fn_all(df, self)

        if transform_fn is None:
            return df

        return transform_fn(df, self)

    def _create_or_replace_historized_mlv(
            self,
            has_schema_changed: bool,
            target_columns_ordered: list[str]
    ) -> None:
        if not has_schema_changed:
            print("MLV: No schema change detected.")
            return
        self._drop_historized_mlv()
        self._create_historized_mlv(target_columns_ordered)

    def _create_historized_mlv(self, target_columns_ordered: list[str]) -> None:
        print(f"MLV: CREATE MLV {self.mlv_name}")
        if not self._is_create_hist_mlv:
            return

        last_columns_ordered = [
            self._row_is_current_column,
            self._row_hist_number_column,
            self._row_update_dts_column,
            self._row_delete_dts_column,
            self._row_load_dts_column
        ]

        silver_columns_ordered_str = ",\n".join([f"`{column}`" for column in target_columns_ordered])

        final_ordered_columns = [
            column
            for column in target_columns_ordered
            if column not in last_columns_ordered
        ] + last_columns_ordered

        final_ordered_columns_str = ",\n".join([f"`{column}`" for column in final_ordered_columns])

        assert len(set(final_ordered_columns)) == len(final_ordered_columns), \
               f"Duplicate columns found in final ordered columns {final_ordered_columns_str}."

        # TODO: wenn ConstantColumns mit part_of_nk, dann muss hier noch nach partitioniert werden!
        constant_column_str = ""
        for constant_column in self._constant_columns:
            if constant_column.part_of_nk:
                constant_column_str = f", `{constant_column.name}`"
                break

        self._mlv_code = f"""
CREATE MATERIALIZED LAKE VIEW {self.mlv_name}
AS
WITH cte_mlv AS (
    SELECT
        {silver_columns_ordered_str}
        ,LEAD({self._row_load_dts_column}) OVER (PARTITION BY {self._nk_column_name} {constant_column_str} ORDER BY {self._row_load_dts_column} DESC) AS {self._row_update_dts_column}
        ,ROW_NUMBER() OVER (PARTITION BY {self._nk_column_name} {constant_column_str} ORDER BY {self._row_load_dts_column} DESC) AS {self._row_hist_number_column}
    FROM {self._dest_table.table_path}
), cte_mlv_final AS (
    SELECT
        *
        ,IIF({self._row_hist_number_column} = 1 AND {self._row_delete_dts_column} IS NULL, 1, 0) AS {self._row_is_current_column}
)
SELECT
{final_ordered_columns_str}
FROM cte_mlv
"""
        if self._is_testing_mock:
            return

        self._spark.sql(self._mlv_code)

    def _drop_historized_mlv(self) -> None:
        drop_mlv_sql = f"DROP MATERIALIZED LAKE VIEW IF EXISTS {self.mlv_name}"
        print(drop_mlv_sql)

        if self._is_testing_mock:
            return

        self._spark.sql(drop_mlv_sql)

    def _refresh_historized_mlv(self) -> None:
        refresh_mlv_sql = f"REFRESH MATERIALIZED LAKE VIEW {self.mlv_name}"
        print(refresh_mlv_sql)

        if self._is_testing_mock:
            return

        self._spark.sql(refresh_mlv_sql)

    def _has_schema_change(self, df_bronze: DataFrame, df_silver: DataFrame) -> bool:
        """Check if the schema of the bronze DataFrame is different from the silver DataFrame.

        Args:
            df_bronze (DataFrame): Bronze DataFrame.
            df_silver (DataFrame): Silver DataFrame.

        Returns:
            bool: True if the schema has changed, False otherwise.
        """
        if df_silver is None:
            return True
        return set(df_bronze.columns) != set(df_silver.columns)

    def _write_df(self, df: DataFrame, write_mode: str) -> None:
        writer = df.write \
            .format("delta") \
            .mode(write_mode) \
            .option("mergeSchema", "true") \
            .partitionBy(*self._partition_by)

        if self._is_testing_mock:
            writer.save(get_mock_table_path(self._dest_table))
            return

        writer.saveAsTable(self._dest_table.table_path)


etl = SilverIngestionInsertOnlyService()
