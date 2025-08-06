# flake8: noqa

import datetime

from typing import Callable
from uuid import uuid4
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@F.udf(returnType=StringType())
def generate_uuid():
    """Generiert eine UUID4"""
    return str(uuid4())


@dataclass(frozen=True)
class ConstantColumn:
    """Class for adding a column with constant value to etl"""
    name: str
    value: str
    part_of_nk: bool = False

    def __post_init__(self):
        """
        Nach initialisierung wird der name in UPPERCASE umgewandelt.
        """
        object.__setattr__(self, "name", self.name.upper())


class SilverIngestionService:
    _is_initialized: bool = False

    def init(
        self,
        *,
        spark: SparkSession,
        src_lakehouse: str,
        src_schema: str,
        src_tablename: str,
        dist_lakehouse: str,
        dist_schema: str,
        dist_tablename: str,
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
        
        pk_column: str = "PK",
        nk_column: str = "NK",
        nk_column_concate_str: str = "_",
        row_is_current_column: str = "ROW_IS_CURRENT",
        row_update_dts_column: str = "ROW_UPDATE_DTS",
        row_delete_dts_column: str = "ROW_DELETE_DTS",
        ldts_column: str = "LDTS",
    ) -> None:
        self._current_timestamp = F.from_utc_timestamp(F.current_timestamp(), "Europe/Berlin")

        self._spark = spark
        self._df_bronze = df_bronze
        self._historize = historize
        self._is_delta_load = is_delta_load
        self._delta_load_use_broadcast = delta_load_use_broadcast
        self._src_lakehouse = src_lakehouse
        self._src_schema = src_schema
        self._src_tablename = src_tablename
        self._dist_lakehouse = dist_lakehouse
        self._dist_schema = dist_schema
        self._dist_tablename = dist_tablename
        self._nk_columns = nk_columns
        self._include_comparing_columns = include_comparing_columns

        self._exclude_comparing_columns = exclude_comparing_columns if isinstance(exclude_comparing_columns, list) else []
        self._transformations: dict[str, Callable] = transformations if isinstance(transformations, dict) else {}
        self._constant_columns: list[ConstantColumn] = constant_columns if isinstance(constant_columns, list) else []        
        self._partition_by: list[str] = partition_by_columns if isinstance(partition_by_columns, list) else []

        self._pk_column = pk_column
        self._nk_column = nk_column
        self._nk_column_concate_str = nk_column_concate_str
        self._row_is_current_column = row_is_current_column
        self._row_update_dts_column = row_update_dts_column
        self._row_delete_dts_column = row_delete_dts_column
        self._ldts_column = ldts_column

        self._sql_src_table = f"{src_lakehouse}.{src_schema}.{src_tablename}"
        self._sql_dist_table = f"{dist_lakehouse}.{dist_schema}.{dist_tablename}"
        
        self._validate_parameters()
        self._set_spark_config()

        self._dw_columns = [
            self._pk_column,
            self._nk_column,
            self._row_is_current_column,
            self._row_update_dts_column,
            self._row_delete_dts_column,
            self._ldts_column
        ]

        self._exclude_comparing_columns = set(
            [self._pk_column]
            + self._nk_columns
            + self._dw_columns
            + self._exclude_comparing_columns
            + [column.name for column in self._constant_columns]
        )

        self._spark.catalog.clearCache()
        self._is_initialized = True

    def __str__(self) -> str:
        if not self._is_initialized:
            return super.__str__(self)
    
        return str({
            "historize": self._historize,
            "is_delta_load": self._is_delta_load,
            "delta_load_use_broadcast": self._delta_load_use_broadcast,
            "src_lakehouse": self._src_lakehouse,
            "src_schema": self._src_schema,
            "src_tablename": self._src_tablename,
            "dist_lakehouse": self._dist_lakehouse,
            "dist_schema": self._dist_schema,
            "dist_tablename": self._dist_tablename,
            "nk_columns": self._nk_columns,
            "include_comparing_columns": self._include_comparing_columns,
            "exclude_comparing_columns": self._exclude_comparing_columns,
            "transformations": self._transformations,
            "constant_columns": self._constant_columns,
            "partition_by": self._partition_by,
            "pk_column": self._pk_column,
            "nk_column": self._nk_column,
            "nk_column_concate_str": self._nk_column_concate_str,
            "row_is_current_column": self._row_is_current_column,
            "row_update_dts_column": self._row_update_dts_column,
            "row_delete_dts_column": self._row_delete_dts_column,
            "ldts_column": self._ldts_column,
            "sql_src_table": self._sql_src_table,
            "sql_dist_table": self._sql_dist_table,
            "dw_columns": self._dw_columns,
            "exclude_comparing_columns": self._exclude_comparing_columns
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
        self._validate_param_isinstance(self._is_delta_load, "is_delta_load", bool)
        self._validate_param_isinstance(self._delta_load_use_broadcast, "delta_load_use_broadcast", bool)
        self._validate_param_isinstance(self._transformations, "transformations", dict)
        self._validate_min_length(self._pk_column, "pk_column", 2)
        self._validate_min_length(self._src_lakehouse, "src_lakehouse", 3)
        self._validate_min_length(self._src_schema, "src_schema", 3)
        self._validate_min_length(self._src_tablename, "src_tablename", 3)
        self._validate_min_length(self._dist_lakehouse, "dist_lakehouse", 3)
        self._validate_min_length(self._dist_schema, "dist_schema", 3)
        self._validate_min_length(self._dist_tablename, "dist_tablename", 3)
        self._validate_min_length(self._nk_columns, "nk_columns", 1)
        self._validate_min_length(self._nk_column, "nk_column", 2)
        self._validate_min_length(self._nk_column_concate_str, "nk_column_concate_str", 1)
        self._validate_param_isinstance(self._exclude_comparing_columns, "exclude_columns_from_comparing", list)

        self._validate_min_length(self._row_delete_dts_column, "row_delete_dts_columnname", 3)
        self._validate_min_length(self._ldts_column, "ldts_column", 3)
        self._validate_param_isinstance(self._constant_columns, "constant_columns", list)

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
                err_msg = f"Invalid items in constant_columns found. All items should be instance of ConstantColumn"
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
        """
        TODO: add doku
        """
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
        
        Setting "spark.sql.legacy.parquet.int96RebaseModeInRead" and "spark.sql.legacy.parquet.int96RebaseModeInWrite" to "CORRECTED" ensures that Int96 values (a specific timestamp representation used in Parquet files) are correctly rebased during both reading and writing operations.
        This is crucial for maintaining consistency and accuracy, especially when dealing with timestamp data across different systems or time zones.
        Similarly, configuring "spark.sql.legacy.parquet.datetimeRebaseModeInRead" and "spark.sql.legacy.parquet.datetimeRebaseModeInWrite" to "CORRECTED" ensures correct handling of datetime values during Parquet file operations.
        By specifying this rebasing mode, potential discrepancies or errors related to datetime representations are mitigated, resulting in more reliable data processing and analysis workflows.
        """
        self._spark.conf.set("spark.sql.parquet.vorder.enabled", "true")

        self._spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        self._spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        self._spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        self._spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

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

        join_condition = (df_bronze[self._nk_column] == df_silver[self._nk_column])
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
        all_columns = [column for column in df.columns if column not in self._dw_columns]

        return [self._pk_column, self._nk_column] + all_columns + [self._ldts_column, self._row_update_dts_column, self._row_delete_dts_column, self._row_is_current_column]

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