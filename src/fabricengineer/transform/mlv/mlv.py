from typing import Any
from pyspark.sql import DataFrame, SparkSession


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
        self._lakehouse = lakehouse
        self._schema = schema
        self._table = table
        self._table_suffix = table_suffix
        self._is_testing_mock = is_testing_mock

        # 'spark' and 'notebookutils' are available in Fabric Notebook
        self._spark = self._get_init_spark(spark_)
        self._notebookutils = self._get_init_notebookutils(notebookutils_)
        return self

    def _get_init_spark(self, spark_: SparkSession) -> SparkSession | None:
        if isinstance(spark_, SparkSession):
            return spark_
        try:
            if spark is not None:  # noqa: F821 # type: ignore
                return spark  # noqa: F821 # type: ignore
            return spark_
        except Exception:
            return None

    def _get_init_notebookutils(self, notebookutils_: Any) -> Any | None:
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
        path = self.file_path
        try:
            if not self.notebookutils.fs.exists(path):
                return None
            if self._is_testing_mock:
                with open(path, 'r') as file:
                    return file.read()
            df = self.spark.read.text(path, wholetext=True)
            mlv_code = df.collect()[0][0]
            return mlv_code
        except Exception as e:
            raise RuntimeError(f"Fehler beim Lesen der Datei: {e}")

    def write_file(self, sql: str) -> bool:
        try:
            result = self.notebookutils.fs.put(
                file=self.file_path,
                content=sql,
                overwrite=True
            )
            return result
        except Exception as e:
            raise RuntimeError(f"Fehler beim Schreiben der Datei: {e}")

    def create_schema(self) -> None:
        create_schema = f"CREATE SCHEMA IF NOT EXISTS {self.schema_path}"
        print(create_schema)

        if self._is_testing_mock:
            return None

        return self.spark.sql(create_schema)

    def create(self, sql: str) -> DataFrame:
        create_mlv = f"CREATE MATERIALIZED LAKE VIEW {self.table_path}\nAS\n{sql}"

        self.create_schema()
        print(f"CREATE MLV: {self.table_path}")
        if self._is_testing_mock:
            return None

        return self.spark.sql(create_mlv)

    def drop(self) -> str:
        drop_mlv = f"DROP MATERIALIZED LAKE VIEW IF EXISTS {self.table_path}"
        print(drop_mlv)

        if self._is_testing_mock:
            return None

        return self.spark.sql(drop_mlv)

    def create_or_replace(self, sql: str, mock_is_existing: bool = None) -> DataFrame:
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
            print("WARN: file=None, is_existing=True. RECREATE.")
            self.drop()
            res = self.create(sql)
            self.write_file(sql)
            return res

        elif sql == mlv_code_current and is_existing:
            print("Nothing has changed.")
            return None

        print(f"REPLACE MLV: {self.table_path}")
        self.drop()
        res = self.create(sql)
        self.write_file(sql)
        return res

    def refresh(self, full_refresh: bool) -> DataFrame:
        full_refresh_str = "FULL" if full_refresh else ""
        refresh_mlv = f"REFRESH MATERIALIZED LAKE VIEW {self.table_path} {full_refresh_str}"
        print(refresh_mlv)

        if self._is_testing_mock:
            return None

        return self.spark.sql(refresh_mlv)

    def to_dict(self) -> None:
        return {
            "lakehouse": self.lakehouse,
            "schema": self.schema,
            "table": self.table,
            "table_path": self.table_path
        }


mlv = MaterializedLakeView()
