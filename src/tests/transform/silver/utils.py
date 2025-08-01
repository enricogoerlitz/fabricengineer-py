from datetime import datetime
from dataclasses import dataclass
from pyspark.sql import SparkSession, functions as F, types as T
from fabricengineer.transform.silver.base import LakehouseTable
from fabricengineer.transform.silver.insertonly import get_mock_save_path


@dataclass
class BronzeDataFrameRecord:
    id: int
    name: str
    created_at: str = "2023-01-01"
    updated_at: str = "2023-01-01"


class BronzeDataFrameDataGenerator:
    def __init__(
        self,
        spark: SparkSession,
        table: LakehouseTable,
        init_record_count: int = 10
    ) -> None:
        self.spark = spark
        self.table = table
        self.init_record_count = init_record_count
        self.df = self._generate_df()

    def _generate_df(self):
        data = [
            (i, f"Name-{i}", "2023-01-01", "2023-01-01")
            for i in range(1, self.init_record_count + 1)
        ]

        schema = T.StructType([
            T.StructField("id", T.IntegerType(), False),
            T.StructField("name", T.StringType(), False),
            T.StructField("created_at", T.StringType(), False),
            T.StructField("updated_at", T.StringType(), False),
        ])

        df_bronze = self.spark.createDataFrame(data, schema)
        df_bronze = df_bronze \
            .withColumn("created_at", F.to_timestamp("created_at")) \
            .withColumn("updated_at", F.to_timestamp("updated_at"))

        return df_bronze

    def write(self) -> 'BronzeDataFrameDataGenerator':
        self.df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(get_mock_save_path(self.table))
        return self

    def read(self) -> 'BronzeDataFrameDataGenerator':
        self.df = self.spark.read \
            .format("parquet") \
            .load(get_mock_save_path(self.table)) \
            .orderBy(F.col("id").asc(), F.col("created_at").asc())
        return self

    def add_records(self, records: list[BronzeDataFrameRecord]) -> 'BronzeDataFrameDataGenerator':
        new_data = [
            (record.id, record.name, datetime.strptime(record.created_at, "%Y-%m-%d"), datetime.strptime(record.updated_at, "%Y-%m-%d"))
            for record in records
        ]
        new_df = self.spark.createDataFrame(new_data, schema=self.df.schema)

        self.df = self.df.union(new_df)
        return self

    def update_records(self, records: list[BronzeDataFrameRecord]) -> 'BronzeDataFrameDataGenerator':
        for record in records:
            self.df = self.df \
                .withColumn(
                    "name",
                    F.when(F.col("id") == record.id, record.name)
                    .otherwise(F.col("name"))
                ) \
                .withColumn(
                    "updated_at",
                    F.when(F.col("id") == record.id, F.current_timestamp()).otherwise(F.col("updated_at"))
                )
        return self

    def delete_records(self, ids: list[int]) -> 'BronzeDataFrameDataGenerator':
        self.df = self.df.filter(~F.col("id").isin(ids))
        return self
