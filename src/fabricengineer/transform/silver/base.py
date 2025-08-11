import os

from typing import Callable
from abc import ABC, abstractmethod
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F
)
from fabricengineer.transform.silver.utils import (
    ConstantColumn,
    get_mock_table_path
)
from fabricengineer.transform.lakehouse import LakehouseTable
from fabricengineer.logging.logger import logger

# base.py


class BaseSilverIngestionService(ABC):

    @abstractmethod
    def init(self, **kwargs): pass

    @abstractmethod
    def run(self, **kwargs): pass

    @abstractmethod
    def read_silver_df(self) -> DataFrame: pass


class BaseSilverIngestionServiceImpl(BaseSilverIngestionService, ABC):
    _is_initialized: bool = False

    @abstractmethod
    def run(self, **kwargs): pass

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
        dw_columns: list[str] = None,

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
        self._is_testing_mock = is_testing_mock

        self._spark = spark_
        self._df_bronze = df_bronze
        self._historize = historize
        self._is_create_hist_mlv = create_historized_mlv
        self._is_delta_load = is_delta_load
        self._delta_load_use_broadcast = delta_load_use_broadcast
        self._src_table = source_table
        self._dest_table = destination_table
        self._nk_columns = nk_columns
        self._include_comparing_columns = include_comparing_columns

        self._exclude_comparing_columns: list[str] = exclude_comparing_columns or []
        self._transformations: dict[str, Callable] = transformations or {}
        self._constant_columns: list[ConstantColumn] = constant_columns or []
        self._partition_by: list[str] = partition_by_columns or []
        self._dw_columns = dw_columns

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

        self._exclude_comparing_columns = set(
            [self._pk_column_name]
            + self._nk_columns
            + self._dw_columns
            + self._exclude_comparing_columns
            + [column.name for column in self._constant_columns]
        )

        self._spark.catalog.clearCache()

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
        self._validate_min_length(self._row_load_dts_column, "row_load_dts_column", 3)
        self._validate_min_length(self._row_hist_number_column, "row_hist_number_column", 3)
        self._validate_min_length(self._row_is_current_column, "row_is_current_column", 3)
        self._validate_min_length(self._row_update_dts_column, "row_update_dts_column", 3)
        self._validate_min_length(self._row_delete_dts_column, "row_delete_dts_column", 3)

        self._validate_transformations()
        self._validate_constant_columns()

    def read_silver_df(self, fformat: str = "delta") -> DataFrame:
        """Reads the silver layer DataFrame.

        Returns:
            DataFrame: The silver layer DataFrame.
        """
        if self._is_testing_mock:
            if not os.path.exists(get_mock_table_path(self._dest_table)):
                return None
        elif not self._spark.catalog.tableExists(self._dest_table.table_path):
            return None

        sql_select_destination = f"SELECT * FROM {self._dest_table.table_path}"

        if self._is_testing_mock:
            df = self._spark.read.format(fformat).load(get_mock_table_path(self._dest_table))
            return df

        df = self._spark.sql(sql_select_destination)
        return df

    def _write_df(self, df: DataFrame, write_mode: str) -> None:
        """Writes the DataFrame to the specified location.

        Args:
            df (DataFrame): The DataFrame to write.
            write_mode (str): The write mode (e.g., "overwrite", "append").
        """
        writer = df.write \
            .format("delta") \
            .mode(write_mode) \
            .option("mergeSchema", "true") \
            .partitionBy(*self._partition_by)

        if self._is_testing_mock:
            writer.save(get_mock_table_path(self._dest_table))
            return

        writer.saveAsTable(self._dest_table.table_path)

    def _create_destination_schema(self) -> None:
        """Creates the destination schema if it does not exist."""
        sql = f"CREATE SCHEMA IF NOT EXISTS {self._dest_table.lakehouse}.{self._dest_table.schema}"
        if not self._is_testing_mock:
            self._spark.sql(sql)

    def _get_columns_ordered(self, df: DataFrame, last_columns: list[str]) -> list[str]:
        """Get the columns in the desired order for processing.

        Args:
            df (DataFrame): The DataFrame to analyze.

        Returns:
            list[str]: The columns in the desired order.
        """
        data_columns = [
            column
            for column in df.columns
            if column not in self._dw_columns
        ]

        return (
            [self._pk_column_name, self._nk_column_name] +
            data_columns +
            last_columns
        )

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """Applies transformations to the DataFrame.
        Uses the source table name to find the appropriate transformation function.
        Or uses a wildcard transformation function if available.

        Args:
            df (DataFrame): The DataFrame to transform.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        transform_fn: Callable = self._transformations.get(self._src_table.table)
        transform_fn_all: Callable = self._transformations.get("*")

        if transform_fn_all is not None:
            df = transform_fn_all(df, self)

        if transform_fn is None:
            return df

        return transform_fn(df, self)

    def _get_columns_to_compare(self, df: DataFrame) -> list[str]:
        """Get the columns to compare in the DataFrame.

        Args:
            df (DataFrame): The DataFrame to analyze.

        Returns:
            list[str]: The columns to compare.
        """
        if (
            isinstance(self._include_comparing_columns, list) and
            len(self._include_comparing_columns) >= 1
        ):
            self._validate_include_comparing_columns(df)
            return self._include_comparing_columns

        comparison_columns = [
            column
            for column in df.columns
            if column not in self._exclude_comparing_columns
        ]

        return comparison_columns

    def _add_missing_columns(self, df_target: DataFrame, df_source: DataFrame) -> DataFrame:
        """Adds missing columns from the source DataFrame to the target DataFrame.

        Args:
            df_target (DataFrame): The target DataFrame to which missing columns will be added.
            df_source (DataFrame): The source DataFrame from which missing columns will be taken.

        Returns:
            DataFrame: The target DataFrame with missing columns added.
        """
        missing_columns = [
            missing_column
            for missing_column in df_source.columns
            if missing_column not in df_target.columns
        ]

        for missing_column in missing_columns:
            df_target = df_target.withColumn(missing_column, F.lit(None))

        return df_target

    def _compare_condition(
        self,
        df_bronze: DataFrame,
        df_silver: DataFrame,
        columns_to_compare: list[str]
    ) -> tuple[F.Column, F.Column]:
        """Compares the specified columns of the bronze and silver DataFrames.

        Args:
            df_bronze (DataFrame): The bronze DataFrame.
            df_silver (DataFrame): The silver DataFrame.
            columns_to_compare (list[str]): The columns to compare.

        Returns:
            tuple[F.Column, F.Column]: The equality and inequality conditions.
        """
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

    def _validate_transformations(self) -> None:
        """Validates the transformation functions.

        Raises:
            TypeError: If any transformation function is not callable.
        """
        for key, fn in self._transformations.items():
            logger.info(f"Transformation function for key '{key}': {fn}")
            if not callable(fn):
                err_msg = f"The transformation function for key '{key}' is not callable."
                raise TypeError(err_msg)

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
        for constant_column in self._constant_columns:
            self._validate_param_isinstance(constant_column, "constant_column", ConstantColumn)

            if constant_column.part_of_nk:
                nk_count += 1

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
        """Validates the include_comparing_columns.

        Args:
            df (DataFrame): The dataframe to validate against.

        Raises:
            ValueError: If include_comparing_columns is empty or if any column in include_comparing_columns
            ValueError: If any column in include_comparing_columns is not present in the dataframe.
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
        """Validates the partition by columns.

        Args:
            df (DataFrame): The dataframe to validate against.

        Raises:
            TypeError: If partition_by is not a list.
            ValueError: If any partition_column is not present in the dataframe.
        """
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
        self._spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    def __str__(self) -> str:
        if not self._is_initialized:
            return super().__str__()

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
            "row_load_dts_column": self._row_load_dts_column,
            "row_update_dts_column": self._row_update_dts_column,
            "row_delete_dts_column": self._row_delete_dts_column,
            "dw_columns": self._dw_columns
        })
