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


# scd2.py


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

        **kwargs
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

            is_testing_mock=kwargs.get("is_testing_mock", False)
        )

        self._validate_scd2_params()

        self._is_initialized = True

    def __str__(self) -> str:
        return super().__str__()

    def _validate_scd2_params(self) -> None:
        pass

    def run(self) -> None:
        """Ingest data from bronze to silver layer.
        This method performs the following steps:
        1. Create a DataFrame from the bronze layer.
        2. Create a DataFrame from the silver layer.
        3. If the silver layer is empty or we are not historizing, overwrite the silver layer with the bronze layer.
        4. If the silver layer is not empty and we are historizing, perform the following steps:
            a. Compare the bronze and silver DataFrames to find new and updated records.
            b. Insert new records into the silver layer.
            c. Update existing records in the silver layer with the new data from the bronze layer.
            d. Set the ROW_UPDATE_DTS and ROW_IS_CURRENT columns for updated records.
            e. Set the ROW_DELETE_DTS and ROW_IS_CURRENT columns for deleted records.
        5. If the silver layer is not empty and we are not historizing, perform the following steps:
            a. Compare the bronze and silver DataFrames to find new, updated, and deleted records.
            b. Insert new records into the silver layer.
            c. Update existing records in the silver layer with the new data from the bronze layer.
            d. Set the ROW_UPDATE_DTS and ROW_IS_CURRENT columns for updated records.
            e. Set the ROW_DELETE_DTS and ROW_IS_CURRENT columns for deleted records.
        6. If the silver layer is not empty and we are performing a delta load, merge the expired and deleted records into the silver layer.

        Raises:
            RuntimeError: If the service is not initialized before calling this method.
            ValueError: If the required columns are not present in the DataFrame.
            TypeError: If the DataFrame is not of the expected type.
            Exception: If any other error occurs during the ingestion process.
        """
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

        self._create_destination_schema()

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

        # New records
        df_new_records = self._filter_new_records(df_joined, df_bronze, df_silver)

        # Updated records to insert (from bronze)
        df_updated_records = self._filter_updated_records(df_joined, df_bronze, updated_filter_condition)

        # 4.
        df_new_data = df_new_records.unionByName(df_updated_records) \
                                    .select(target_columns_ordered) \
                                    .dropDuplicates(["PK"])

        # self._write_df(df_new_data, "append")

        # 5.
        # Expired records are the records in silver layer, which was updated. Merge/Update them and
        # add current timestamp to ROW_UPDATE_DTS and set ROW_IS_CURRENT to 0.
        df_expired_records = self._filter_expired_records(df_joined, df_silver, updated_filter_condition)

        df_merge_into_records = df_new_data.unionByName(df_expired_records) \
                                           .select(target_columns_ordered) \
                                           .dropDuplicates(["PK"])

        # 6.
        if self._is_delta_load:
            self._exec_merge_into(df_merge_into_records, target_columns_ordered)
            return

        df_deleted_records = self._filter_deleted_records(df_joined, df_bronze, df_silver)

        df_merge_into_records = df_merge_into_records.unionByName(df_deleted_records) \
                                                     .select(target_columns_ordered)

        self._exec_merge_into(df_merge_into_records, target_columns_ordered)

    def _generate_dataframes(self) -> tuple[DataFrame, DataFrame]:
        """Generates the bronze and silver DataFrames.
        This method creates the bronze DataFrame from the source table and the silver DataFrame from the destination table.
        If the silver DataFrame does not exist, it returns None for the silver DataFrame.
        If the bronze DataFrame does not exist, it raises an error.
        Raises:
            RuntimeError: If the bronze DataFrame cannot be created.
            ValueError: If the required columns are not present in the DataFrame.
            TypeError: If the DataFrame is not of the expected type.
            Exception: If any other error occurs during the ingestion process.

        Returns:
            tuple[DataFrame, DataFrame]: The bronze and silver DataFrames.
        """
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
        """Generates the filter condition for updated records.
        This method creates a filter condition that checks for updated records based on the non-key columns.
        It checks if the non-key columns in the bronze DataFrame are not null, the non-key columns in the silver DataFrame are not null,
        the ROW_IS_CURRENT column in the silver DataFrame is equal to 1, and the non-equal condition is met.
        This filter condition is used to identify records that have been updated in the bronze DataFrame compared to the silver DataFrame.

        Args:
            df_bronze (DataFrame): The bronze DataFrame containing the source data.
            df_silver (DataFrame): The silver DataFrame containing the target data.
            neq_condition (Column): The non-equal condition to check for updates.

        Returns:
            Column: The filter condition for updated records.
        """
        updated_filter = (
            (df_bronze[self._nk_column_name].isNotNull()) &
            (df_silver[self._nk_column_name].isNotNull()) &
            (df_silver[self._row_is_current_column] == 1) &
            (neq_condition)
        )

        return updated_filter

    def _filter_new_records(self, df_joined: DataFrame, df_bronze: DataFrame, df_silver: DataFrame) -> DataFrame:
        """Filters new records from the joined DataFrame.
        This method filters the joined DataFrame to find records that are new, meaning they do not exist in the silver DataFrame.
        It checks if the NK column in the silver DataFrame is null, indicating that these records are not present in the silver layer.
        The new records are selected from the bronze DataFrame, which contains the source data.

        Args:
            df_joined (DataFrame): The joined DataFrame containing all records.
            df_bronze (DataFrame): The bronze DataFrame containing the source data.
            df_silver (DataFrame): The silver DataFrame containing the target data.

        Returns:
            DataFrame: A DataFrame containing the new records.
        """
        new_records_filter = (df_silver[self._nk_column_name].isNull())
        df_new_records = df_joined.filter(new_records_filter) \
                                  .select(df_bronze["*"])

        return df_new_records

    def _filter_updated_records(self, df_joined: DataFrame, df_bronze: DataFrame, updated_filter) -> DataFrame:
        """Filters updated records from the joined DataFrame.
        This method filters the joined DataFrame to find records that have been updated in the bronze DataFrame compared to the silver DataFrame.
        It uses the updated filter condition to select records that have been modified.

        Args:
            df_joined (DataFrame): The joined DataFrame containing all records.
            df_bronze (DataFrame): The bronze DataFrame containing the source data.
            updated_filter (Column): The filter condition for updated records.

        Returns:
            DataFrame: A DataFrame containing the updated records.
        """
        # Select not matching bronze columns
        df_updated_records = df_joined.filter(updated_filter) \
                                      .select(df_bronze["*"])

        return df_updated_records

    def _filter_expired_records(self, df_joined: DataFrame, df_silver: DataFrame, updated_filter) -> DataFrame:
        """Filters expired records from the joined DataFrame.
        This method filters the joined DataFrame to find records that have been updated in the silver DataFrame.
        It selects records that match the updated filter condition and updates the ROW_UPDATE_DTS column with the current timestamp.
        It also sets the ROW_DELETE_DTS column to None and the ROW_IS_CURRENT column to 0, indicating that these records are no longer current.
        This is used to mark records in the silver layer as expired when they have been updated in the bronze layer.

        Args:
            df_joined (DataFrame): The joined DataFrame containing all records.
            df_silver (DataFrame): The silver DataFrame containing the target data.
            updated_filter (Column): The filter condition for updated records.

        Returns:
            DataFrame: A DataFrame containing the expired records.
        """
        # Select not matching silver columns
        df_expired_records = df_joined.filter(updated_filter) \
                                      .select(df_silver["*"]) \
                                      .withColumn(self._row_update_dts_column, F.lit(self._current_timestamp)) \
                                      .withColumn(self._row_delete_dts_column, F.lit(None).cast("timestamp")) \
                                      .withColumn(self._row_is_current_column, F.lit(0))

        return df_expired_records

    def _filter_deleted_records(self, df_joined: DataFrame, df_bronze: DataFrame, df_silver: DataFrame) -> DataFrame:
        """Filters deleted records from the joined DataFrame.
        This method filters the joined DataFrame to find records that exist in the silver DataFrame but do not exist in the bronze DataFrame.
        It checks if the NK column in the bronze DataFrame is null, indicating that these records have been deleted.
        The deleted records are selected from the silver DataFrame, which contains the target data.
        It updates the ROW_UPDATE_DTS and ROW_DELETE_DTS columns with the current timestamp
        and sets the ROW_IS_CURRENT column to 0, indicating that these records are no longer current.

        Args:
            df_joined (DataFrame): The joined DataFrame containing all records.
            df_bronze (DataFrame): The bronze DataFrame containing the source data.
            df_silver (DataFrame): The silver DataFrame containing the target data.

        Returns:
            DataFrame: A DataFrame containing the deleted records.
        """
        df_deleted_records = df_joined.filter(df_bronze[self._nk_column_name].isNull()) \
                                      .select(df_silver["*"]) \
                                      .withColumn(self._row_update_dts_column, F.lit(self._current_timestamp)) \
                                      .withColumn(self._row_delete_dts_column, F.lit(self._current_timestamp)) \
                                      .withColumn(self._row_is_current_column, F.lit(0))

        return df_deleted_records

    def _create_bronze_df(self) -> DataFrame:
        """Creates the bronze DataFrame.
        This method reads data from the source table and applies necessary transformations to create the bronze DataFrame.
        It selects all columns from the source table, adds primary key (PK) and natural key (NK) columns,
        and sets the row update, delete, and load timestamps.

        Returns:
            DataFrame: The bronze DataFrame containing the source data.
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
               .withColumn(self._row_update_dts_column, F.lit(None).cast("timestamp")) \
               .withColumn(self._row_delete_dts_column, F.lit(None).cast("timestamp")) \
               .withColumn(self._row_is_current_column, F.lit(1)) \
               .withColumn(self._row_load_dts_column, F.lit(self._current_timestamp))

        return df

    def _create_silver_df(self) -> DataFrame:
        """Creates the silver DataFrame.
        This method reads data from the destination table and applies necessary transformations to create the silver DataFrame.
        It filters the DataFrame to include only current records (ROW_IS_CURRENT = 1)
        and ensures that the natural key (NK) columns are present.

        Returns:
            DataFrame: The silver DataFrame containing the target data.
        """
        if self._is_testing_mock:
            if not os.path.exists(get_mock_table_path(self._dest_table)):
                return None
        elif not self._spark.catalog.tableExists(self._dest_table.table_path):
            return None

        df = self.read_silver_df()
        df = df.filter(F.col(self._row_is_current_column) == 1)

        self._validate_nk_columns_in_df(df)

        for constant_column in self._constant_columns:
            if constant_column.name not in df.columns:
                df = df.withColumn(constant_column.name, F.lit(None))

            if constant_column.part_of_nk:
                df = df.filter(F.col(constant_column.name) == constant_column.value)

        df = df.withColumn(self._nk_column_name, F.concat_ws(self._nk_column_concate_str, *self._nk_columns))

        return df

    def _exec_merge_into(self, df_merge: DataFrame, target_columns_ordered: list[str]) -> None:
        """Executes the MERGE INTO statement.
        - Updates existing rows (by PK) für ROW_UPDATE_DTS, ROW_DELETE_DTS, ROW_IS_CURRENT.
        - Inserts neue Rows (by NOT MATCHED) mit allen in target_columns_ordered verfügbaren Spalten aus df_merge.

        Args:
            df_merge (DataFrame): Source-DataFrame, das gemerged werden soll.
            target_columns_ordered (list[str]): Zielspalten-Reihenfolge; wird für INSERT verwendet.
        """
        # Sicherheitsnetz: nur Spalten inserten, die im Source auch vorhanden sind
        insert_cols = [c for c in target_columns_ordered if c in df_merge.columns]
        if not insert_cols:
            raise ValueError("Keine der target_columns_ordered-Spalten ist im df_merge vorhanden; INSERT wäre leer.")

        if self._is_testing_mock:
            # DeltaTable-API (Mock / File-Path)
            target_path = get_mock_table_path(self._dest_table)
            delta_table = DeltaTable.forPath(self._spark, target_path)

            # Update-Set nur für die drei ROW_* Spalten (wie bisher)
            update_set = {
                self._row_update_dts_column: f"source.{self._row_update_dts_column}",
                self._row_delete_dts_column: f"source.{self._row_delete_dts_column}",
                self._row_is_current_column: f"source.{self._row_is_current_column}"
            }

            # Insert-Set für alle gewünschten Spalten
            insert_set = {col: f"source.{col}" for col in insert_cols}

            (
                delta_table.alias("target")
                .merge(
                    df_merge.alias("source"),
                    f"target.{self._pk_column_name} = source.{self._pk_column_name}"
                )
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsert(values=insert_set)
                .execute()
            )
            return

        # SQL-Pfad (Katalogtabellen)
        destination_table_path = self._dest_table.table_path

        # Temp-View-Name (einfach, robust, ohne Backticks)
        view_name = destination_table_path.replace('.', '_').replace('`', '') + "_view"
        df_merge.createOrReplaceTempView(view_name)

        # INSERT-Spaltenliste und VALUES dynamisch aus insert_cols
        cols_sql = ", ".join(f"`{c}`" for c in insert_cols)
        vals_sql = ", ".join(f"source.`{c}`" for c in insert_cols)

        # MERGE-SQL
        self._spark.sql(f"""
            MERGE INTO {destination_table_path} AS target
            USING {view_name} AS source
            ON target.`{self._pk_column_name}` = source.`{self._pk_column_name}`
            WHEN MATCHED THEN
            UPDATE SET
                `{self._row_update_dts_column}` = source.`{self._row_update_dts_column}`,
                `{self._row_delete_dts_column}` = source.`{self._row_delete_dts_column}`,
                `{self._row_is_current_column}` = source.`{self._row_is_current_column}`
            WHEN NOT MATCHED THEN
            INSERT ({cols_sql})
            VALUES ({vals_sql})
        """)


etl = SilverIngestionSCD2Service()
