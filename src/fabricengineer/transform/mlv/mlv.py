from typing import Any
from pyspark.sql import DataFrame, SparkSession
from fabricengineer.logging.logger import logger


# mlv.py


def to_spark_sql(sql: str) -> str:
    return sql \
            .replace("[", "`") \
            .replace("]", "`")


class MaterializedLakeView:
    def __init__(
        self,
        lakehouse: str = None,
        schema: str = None,
        table: str = None,
        table_suffix: str = "_mlv",
        spark_: SparkSession = None,
        notebookutils_: Any = None,
        is_testing_mock: bool = False
    ) -> None:
        self.init(
            lakehouse=lakehouse,
            schema=schema,
            table=table,
            table_suffix=table_suffix,
            spark_=spark_,
            notebookutils_=notebookutils_,
            is_testing_mock=is_testing_mock
        )

    def init(
        self,
        lakehouse: str,
        schema: str,
        table: str,
        table_suffix: str = "_mlv",
        spark_: SparkSession = None,
        notebookutils_: Any = None,
        is_testing_mock: bool = False
    ) -> 'MaterializedLakeView':
        """Initializes the MaterializedLakeView instance.

        Args:
            lakehouse (str): The lakehouse name.
            schema (str): The schema name.
            table (str): The table name.
            table_suffix (str, optional): The table suffix. Defaults to "_mlv".
            spark_ (SparkSession, optional): The SparkSession instance. Defaults to None.
            notebookutils_ (Any, optional): The NotebookUtils instance. Defaults to None.
            is_testing_mock (bool, optional): Whether the instance is a testing mock. Defaults to False.

        Returns:
            MaterializedLakeView: The initialized MaterializedLakeView instance.
        """
        self._lakehouse = lakehouse
        self._schema = schema
        self._table = table
        self._table_suffix = table_suffix
        self._is_testing_mock = is_testing_mock

        # 'spark' and 'notebookutils' are available in Fabric notebook
        self._spark = self._get_init_spark(spark_)
        self._notebookutils = self._get_init_notebookutils(notebookutils_)
        return self

    def _get_init_spark(self, spark_: SparkSession) -> SparkSession | None:
        """Initializes the SparkSession instance.
        If a SparkSession is provided, it is returned. Otherwise, it tries to use the global 'spark' variable.

        Args:
            spark_ (SparkSession): The SparkSession instance.

        Returns:
            SparkSession | None: The initialized SparkSession instance or None.
        """
        if isinstance(spark_, SparkSession):
            return spark_
        try:
            if spark is not None:  # noqa: F821 # type: ignore
                return spark  # noqa: F821 # type: ignore
            return spark_
        except Exception:
            return None

    def _get_init_notebookutils(self, notebookutils_: Any) -> Any | None:
        """Initializes the NotebookUtils instance.
        If a NotebookUtils instance is provided, it is returned. Otherwise, it tries to use the global 'notebookutils' variable.

        Args:
            notebookutils_ (Any): The NotebookUtils instance.

        Returns:
            Any | None: The initialized NotebookUtils instance or None.
        """
        if notebookutils_ is not None:
            return notebookutils_
        try:
            if notebookutils is not None:  # noqa: F821 # type: ignore
                return notebookutils  # noqa: F821 # type: ignore
            return None
        except Exception:
            return None

    @property
    def lakehouse(self) -> str:
        if self._lakehouse is None:
            raise ValueError("Lakehouse is not initialized.")
        return self._lakehouse

    @property
    def schema(self) -> str:
        if self._schema is None:
            raise ValueError("Schema is not initialized.")
        return self._schema

    @property
    def table(self) -> str:
        if self._table is None:
            raise ValueError("Table is not initialized.")
        return self._table

    @property
    def table_suffix(self) -> str:
        return self._table_suffix

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            raise ValueError("SparkSession is not initialized.")
        return self._spark

    @property
    def notebookutils(self) -> Any:
        if self._notebookutils is None:
            raise ValueError("NotebookUtils is not initialized.")
        return self._notebookutils

    @property
    def table_name(self) -> str:
        table_suffix = self.table_suffix or ""
        return f"{self.table}{table_suffix}"

    @property
    def file_path(self) -> str:
        path = f"Files/mlv/{self.lakehouse}/{self.schema}/{self.table_name}.sql.txt"
        return path

    @property
    def table_path(self) -> str:
        table_path = f"{self.lakehouse}.{self.schema}.{self.table_name}"
        return table_path

    @property
    def schema_path(self) -> str:
        schema_path = f"{self.lakehouse}.{self.schema}"
        return schema_path

    def read_file(self) -> str | None:
        """Reads the content of the SQL file from the specified lakehouse.
        If the file does not exist, it returns None.

        Raises:
            RuntimeError: If the file cannot be read.

        Returns:
            str | None: The content of the file or None if it doesn't exist.
        """
        path = self.file_path
        try:
            if not self.notebookutils.fs.exists(path):
                return None
            if self._is_testing_mock:
                with open(path, "r") as file:
                    return file.read()
            df = self.spark.read.text(path, wholetext=True)
            mlv_code = df.collect()[0][0]
            return mlv_code
        except Exception as e:
            raise RuntimeError(f"Fehler beim Lesen der Datei: {e}")

    def write_file(self, sql: str) -> bool:
        """Writes the SQL content to the specified file in a lakehouse.
        If the file already exists, it will be overwritten.
        If the file cannot be written, it raises a RuntimeError.

        Args:
            sql (str): The SQL content to write.

        Raises:
            RuntimeError: If the file cannot be written.

        Returns:
            bool: True if the file was written successfully, False otherwise.
        """
        try:
            result = self.notebookutils.fs.put(
                file=self.file_path,
                content=sql,
                overwrite=True
            )
            return result
        except Exception as e:
            raise RuntimeError(f"Fehler beim Schreiben der Datei: {e}")

    def create_schema(self) -> DataFrame | None:
        """Creates the schema in the lakehouse if it does not exist."""
        create_schema = f"CREATE SCHEMA IF NOT EXISTS {self.schema_path}"
        logger.info(create_schema)

        if self._is_testing_mock:
            return None

        return self.spark.sql(create_schema)

    def create(self, sql: str) -> DataFrame | None:
        """Creates a Materialized Lake View (MLV) in the lakehouse with the given SQL query."""
        self.create_schema()

        create_mlv = f"CREATE MATERIALIZED LAKE VIEW {self.table_path}\nAS\n{sql}"
        logger.info(f"CREATE MLV: {self.table_path}")

        if self._is_testing_mock:
            return None

        return self.spark.sql(create_mlv)

    def drop(self) -> str:
        """Drops the Materialized Lake View (MLV) if it exists."""
        drop_mlv = f"DROP MATERIALIZED LAKE VIEW IF EXISTS {self.table_path}"
        logger.info(drop_mlv)

        if self._is_testing_mock:
            return None

        return self.spark.sql(drop_mlv)

    def create_or_replace(self, sql: str, mock_is_existing: bool = None) -> DataFrame:
        """Creates or replaces the Materialized Lake View (MLV) in the lakehouse.

        Args:
            sql (str): The SQL query to create the MLV.
            mock_is_existing (bool, optional): If True, it simulates the existence of the MLV. Defaults to None.

        Returns:
            DataFrame: The result of the create or replace operation.
        """
        mlv_code_current = self.read_file()
        is_existing = (
            mock_is_existing
            if mock_is_existing is not None
            else self.spark.catalog.tableExists(self.table_path)
        )

        if mlv_code_current is None and not is_existing:
            res = self.create(sql)
            self.write_file(sql)
            return res

        elif mlv_code_current is None and is_existing:
            logger.warning("WARN: file=None, is_existing=True. RECREATE.")
            self.drop()
            res = self.create(sql)
            self.write_file(sql)
            return res

        elif sql == mlv_code_current and is_existing:
            logger.info("Nothing has changed.")
            return None

        logger.info(f"REPLACE MLV: {self.table_path}")
        self.drop()
        res = self.create(sql)
        self.write_file(sql)
        return res

    def refresh(self, full_refresh: bool) -> DataFrame:
        """Refreshes the Materialized Lake View (MLV) in the lakehouse."""
        full_refresh_str = "FULL" if full_refresh else ""
        refresh_mlv = f"REFRESH MATERIALIZED LAKE VIEW {self.table_path} {full_refresh_str}"
        logger.info(refresh_mlv)

        if self._is_testing_mock:
            return None

        return self.spark.sql(refresh_mlv)

    def to_dict(self) -> None:
        """Returns a dictionary representation of the Materialized Lake View."""
        return {
            "lakehouse": self.lakehouse,
            "schema": self.schema,
            "table": self.table,
            "table_path": self.table_path
        }


mlv = MaterializedLakeView()
