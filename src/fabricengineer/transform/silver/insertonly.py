import os

from typing import Any
from datetime import datetime
from pyspark.sql import (
    SparkSession,
    DataFrame,
    functions as F,
    Window
)
from fabricengineer.transform.silver.utils import (
    ConstantColumn,
    generate_uuid,
    get_mock_table_path
)
from fabricengineer.transform.lakehouse import LakehouseTable
from fabricengineer.transform.silver.base import BaseSilverIngestionServiceImpl
from fabricengineer.transform.mlv import MaterializedLakeView
from fabricengineer.logging.logger import logger


# insertonly.py


class SilverIngestionInsertOnlyService(BaseSilverIngestionServiceImpl):
    _is_initialized: bool = False
    _mlv_code: str | None = None
    _mlv_suffix: str = "_h"

    def init(
        self,
        *,
        spark_: SparkSession,
        notebookutils_: Any = None,
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

        **kwargs
    ) -> None:
        self._mlv_suffix = mlv_suffix

        dw_columns = [
            pk_column_name,
            nk_column_name,
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
            create_historized_mlv=create_historized_mlv,
            dw_columns=dw_columns,

            pk_column_name=pk_column_name,
            nk_column_name=nk_column_name,
            nk_column_concate_str=nk_column_concate_str,
            row_is_current_column=row_is_current_column,
            row_hist_number_column=row_hist_number_column,
            row_update_dts_column=row_update_dts_column,
            row_delete_dts_column=row_delete_dts_column,
            row_load_dts_column=row_load_dts_column,

            is_testing_mock=kwargs.get("is_testing_mock", False)
        )

        self._validate_insertonly_params()

        self._mlv = MaterializedLakeView(
            lakehouse=self._dest_table.lakehouse,
            schema=self._dest_table.schema,
            table=self._dest_table.table,
            table_suffix=self._mlv_suffix,
            spark_=self._spark,
            notebookutils_=notebookutils_,
            is_testing_mock=self._is_testing_mock
        )

        self._is_initialized = True

    @property
    def mlv_name(self) -> str:
        return self._mlv.table_path

    @property
    def mlv_code(self) -> str:
        return self._mlv_code

    def _validate_insertonly_params(self) -> None:
        if self._mlv_suffix is not None:
            self._validate_param_isinstance(self._mlv_suffix, "mlv_suffix", str)
            self._validate_min_length(self._mlv_suffix, "mlv_suffix", 1)

    def __str__(self) -> str:
        s = super().__str__()
        s += f", mlv_suffix: {self._mlv_suffix}"
        return s

    def run(self) -> DataFrame:
        """Ingests data into the silver layer by using the an insert only strategy.

        Raises:
            RuntimeError: If the service is not initialized.

        Returns:
            DataFrame: The ingested silver layer dataframe.
        """
        if not self._is_initialized:
            raise RuntimeError("The SilverIngestionInsertOnlyService is not initialized. Call the init method first.")

        self._current_timestamp = datetime.now()

        # 1.
        df_bronze, df_silver, has_schema_changed = self._generate_dataframes()

        target_columns_ordered = self._get_columns_ordered(df_bronze, last_columns=[
            self._row_load_dts_column,
            self._row_delete_dts_column
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

        self._create_destination_schema()

        if do_overwrite:
            df_inital_load = df_bronze.select(target_columns_ordered)
            self._write_df(df_inital_load, "overwrite")
            self._manage_historized_mlv(has_schema_changed, target_columns_ordered)
            return df_inital_load

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
        df_data_to_insert = df_new_records.unionByName(df_updated_records) \
                                          .select(target_columns_ordered) \
                                          .dropDuplicates(["PK"])

        # 6.
        if self._is_delta_load:
            self._write_df(df_data_to_insert, "append")
            self._manage_historized_mlv(has_schema_changed, target_columns_ordered)
            return df_data_to_insert

        # 7.
        df_deleted_records = self._filter_deleted_records(df_joined, df_bronze, df_silver).select(target_columns_ordered)
        df_data_to_insert = df_data_to_insert.unionByName(df_deleted_records)
        self._write_df(df_data_to_insert, "append")
        self._manage_historized_mlv(has_schema_changed, target_columns_ordered)
        return df_data_to_insert

    def _generate_dataframes(self) -> tuple[DataFrame, DataFrame, bool]:
        """Generates the bronze and silver DataFrames and detects schema changes.

        Returns:
            tuple[DataFrame, DataFrame, bool]: The bronze DataFrame, silver DataFrame, and a boolean indicating if the schema has changed.
        """
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

    def _updated_filter(
        self,
        df_bronze: DataFrame,
        df_silver: DataFrame,
        neq_condition: F.Column
    ) -> F.Column:
        """Creates a filter for updated records.

        Args:
            df_bronze (DataFrame): The bronze DataFrame.
            df_silver (DataFrame): The silver DataFrame.
            neq_condition (Column): not equal condition for the columns to compare.

        Returns:
            Column: The updated filter condition.
        """
        updated_filter = (
            (df_bronze[self._nk_column_name].isNotNull()) &
            (df_silver[self._nk_column_name].isNotNull()) &
            (neq_condition)
        )

        return updated_filter

    def _filter_new_records(
        self,
        df_joined: DataFrame,
        df_bronze: DataFrame,
        df_silver: DataFrame
    ) -> DataFrame:
        """Filters new records from the joined DataFrame.

        Args:
            df_joined (DataFrame): The outer joined DataFrame.
            df_bronze (DataFrame): The bronze DataFrame.
            df_silver (DataFrame): The silver DataFrame.

        Returns:
            DataFrame: The filtered DataFrame containing new records.
        """
        new_records_filter = (df_silver[self._nk_column_name].isNull())
        df_new_records = df_joined.filter(new_records_filter) \
                                  .select(df_bronze["*"])

        return df_new_records

    def _filter_updated_records(
        self,
        df_joined: DataFrame,
        df_bronze: DataFrame,
        updated_filter: F.Column
    ) -> DataFrame:
        """Filters updated records from the joined DataFrame.

        Args:
            df_joined (DataFrame): The outer joined DataFrame.
            df_bronze (DataFrame): The bronze DataFrame.
            updated_filter (Column): The filter condition for updated records.

        Returns:
            DataFrame: The filtered DataFrame containing updated records.
        """
        # Select not matching bronze columns
        df_updated_records = df_joined.filter(updated_filter) \
                                      .select(df_bronze["*"])

        return df_updated_records

    def _filter_deleted_records(
        self,
        df_joined: DataFrame,
        df_bronze: DataFrame,
        df_silver: DataFrame
    ) -> DataFrame:
        """Filters deleted records from the joined DataFrame.

        Args:
            df_joined (DataFrame): The outer joined DataFrame.
            df_bronze (DataFrame): The bronze DataFrame.
            df_silver (DataFrame): The silver DataFrame.

        Returns:
            DataFrame: The filtered DataFrame containing deleted records.
        """
        filter_condition = (df_bronze[self._nk_column_name].isNull()) & (df_silver[self._row_delete_dts_column].isNull())
        df_deleted_records = df_joined.filter(filter_condition) \
                                      .select(df_silver["*"]) \
                                      .withColumn(self._pk_column_name, generate_uuid()) \
                                      .withColumn(self._row_delete_dts_column, F.lit(self._current_timestamp)) \
                                      .withColumn(self._row_load_dts_column, F.lit(self._current_timestamp))

        return df_deleted_records

    def _create_bronze_df(self) -> DataFrame:
        """Creates the bronze DataFrame.
        Adds primary key, natural key, row load timestamp, and row delete timestamp columns.
        If the DataFrame is already provided, it uses that; otherwise, it reads from the source table.
        If the DataFrame is not provided and the source table does not exist, it raises an error.
        If the DataFrame is provided, it validates that it contains all natural key columns.
        If the DataFrame is not provided, it reads from the source table or mock path.
        If the DataFrame is provided, it adds constant columns if they are not already present.
        If the DataFrame is not provided, it reads from the source table or mock path and adds constant columns.

        Returns:
            DataFrame: The bronze DataFrame.
        """
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
        """Creates the silver DataFrame.
        Reads the silver table if it exists, or returns None if it does not.
        If the DataFrame is not provided, it reads from the destination table or mock path.
        Validates that the DataFrame contains all natural key columns.
        Adds constant columns if they are not already present.
        Filters the DataFrame by constant columns that are part of the natural key.
        Concatenates the natural key columns into a single column.

        Returns:
            DataFrame: The silver DataFrame.
        """
        fformat = "delta" if not self._is_testing_mock else "parquet"
        if self._is_testing_mock:
            if not os.path.exists(get_mock_table_path(self._dest_table)):
                return None
        elif not self._spark.catalog.tableExists(self._dest_table.table_path):
            return None

        df = self.read_silver_df(fformat=fformat)

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

        current_record_filter = (
            (F.col("ROW_NUMBER") == 1) &
            (F.col(self._row_delete_dts_column).isNull())
        )
        df = df_with_rownum.filter(current_record_filter).select(return_columns)

        return df

    def _manage_historized_mlv(
            self,
            has_schema_changed: bool,
            target_columns_ordered: list[str]
    ) -> None:
        """Manages the historized materialized lake view (MLV) creation, replacement, and refresh.

        Args:
            has_schema_changed (bool): Indicates if the schema has changed.
            target_columns_ordered (list[str]): The ordered list of target columns.
        """
        if not self._is_create_hist_mlv:
            logger.info("MLV: Historized MLV creation is disabled.")
            return

        self._create_or_replace_historized_mlv(has_schema_changed, target_columns_ordered)
        self._mlv.refresh(full_refresh=False)

    def _create_or_replace_historized_mlv(
            self,
            has_schema_changed: bool,
            target_columns_ordered: list[str]
    ) -> None:
        """Creates or replaces the historized materialized lake view (MLV).

        Args:
            has_schema_changed (bool): Indicates if the schema has changed.
            target_columns_ordered (list[str]): The ordered list of target columns.
        """
        is_mlv_existing = (
            self._spark.catalog.tableExists(self._mlv.table_path)
            if not self._is_testing_mock else True
        )
        if not has_schema_changed and is_mlv_existing:
            logger.info("MLV: No schema change detected.")
            return

        if is_mlv_existing:
            self._mlv.drop()
        self._create_historized_mlv(target_columns_ordered)

    def _create_historized_mlv(self, target_columns_ordered: list[str]) -> None:
        """
        Creates a historized materialized lake view (MLV).

        Args:
            target_columns_ordered (list[str]): The ordered list of target columns for the MLV
        """
        logger.info(f"MLV: CREATE MLV {self.mlv_name}")

        silver_columns_ordered_str = self._mlv_silver_columns_ordered_str(target_columns_ordered)
        final_ordered_columns_str = self._mlv_final_column_order_str(target_columns_ordered)
        constant_column_str = self._mlv_constant_column_str()

        self._mlv_code = self._generate_mlv_code(
            silver_columns_ordered_str,
            final_ordered_columns_str,
            constant_column_str
        )

        self._mlv.create(self._mlv_code)

    def _generate_mlv_code(
            self,
            silver_columns_ordered_str: str,
            final_ordered_columns_str: str,
            constant_column_str: str
    ) -> str:
        """Generates the SQL code for creating a materialized lake view (MLV)."""
        return f"""
WITH cte_mlv AS (
    SELECT
        {silver_columns_ordered_str}
        ,LAG({self._row_load_dts_column}) OVER (PARTITION BY {self._nk_column_name} {constant_column_str} ORDER BY {self._row_load_dts_column} DESC) AS {self._row_update_dts_column}
        ,ROW_NUMBER() OVER (PARTITION BY {self._nk_column_name} {constant_column_str} ORDER BY {self._row_load_dts_column} DESC) AS {self._row_hist_number_column}
    FROM {self._dest_table.table_path}
), cte_mlv_final AS (
    SELECT
        *
        ,CASE
            WHEN {self._row_hist_number_column} = 1 AND {self._row_delete_dts_column} IS NULL THEN 1
            ELSE 0
        END AS {self._row_is_current_column}
    FROM cte_mlv
)
SELECT
{final_ordered_columns_str}
FROM cte_mlv_final
"""

    def _mlv_silver_columns_ordered_str(self, target_columns_ordered: list[str]) -> str:
        """Generates a string representation of the silver columns for the materialized lake view (MLV).

        Args:
            target_columns_ordered (list[str]): The ordered list of target columns.

        Returns:
            _type_: _description_
        """
        silver_columns_ordered_str = ",\n".join([f"`{column}`" for column in target_columns_ordered])
        return silver_columns_ordered_str

    def _mlv_final_column_order_str(self, target_columns_ordered: list[str]) -> str:
        """Generates a string representation of the final column order for the materialized lake view (MLV).

        Args:
            target_columns_ordered (list[str]): The ordered list of target columns.

        Returns:
            str: A string representation of the final column order for the MLV.
        """
        last_columns_ordered = [
            self._row_load_dts_column,
            self._row_update_dts_column,
            self._row_delete_dts_column,
            self._row_is_current_column,
            self._row_hist_number_column
        ]
        final_ordered_columns = [
            column
            for column in target_columns_ordered
            if column not in last_columns_ordered
        ] + last_columns_ordered

        final_ordered_columns_str = ",\n".join([f"`{column}`" for column in final_ordered_columns])

        assert len(set(final_ordered_columns)) == len(final_ordered_columns), \
               f"Duplicate columns found in final ordered columns {final_ordered_columns_str}."

        return final_ordered_columns_str

    def _mlv_constant_column_str(self) -> str:
        """Generates a string representation of the constant columns that are part of the natural key (NK).

        Returns:
            str: A string representation of the constant columns for use in MLV creation.
        """
        constant_column_str = ""
        for constant_column in self._constant_columns:
            if constant_column.part_of_nk:
                constant_column_str = f", `{constant_column.name}`"
                break
        return constant_column_str

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


etl = SilverIngestionInsertOnlyService()
