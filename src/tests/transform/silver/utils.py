from datetime import datetime
from dataclasses import dataclass, field
from pyspark.sql import SparkSession, functions as F, types as T
from fabricengineer.transform.lakehouse import LakehouseTable
from fabricengineer.transform.silver.insertonly import get_mock_table_path


@dataclass
class BronzeDataFrameRecord:
    id: int
    name: str
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    ncol: str = None


class BronzeDataFrameDataGenerator:
    def __init__(
        self,
        spark: SparkSession,
        table: LakehouseTable,
        init_data: list[BronzeDataFrameRecord] = None,
        init_name_prefix: str = "Name-"
    ) -> None:
        self.spark = spark
        self.table = table
        self.init_data = init_data or []
        self.init_name_prefix = init_name_prefix
        self.df = self._generate_df(self.init_data)

    def _generate_df(self, init_data: list[BronzeDataFrameRecord]):
        data = [
            (record.id, record.name, record.created_at, record.updated_at)
            for record in init_data
        ]

        schema = T.StructType([
            T.StructField("id", T.IntegerType(), False),
            T.StructField("name", T.StringType(), False),
            T.StructField("created_at", T.TimestampType(), False),
            T.StructField("updated_at", T.TimestampType(), False),
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
            .save(get_mock_table_path(self.table))
        return self

    def read(self) -> 'BronzeDataFrameDataGenerator':
        self.df = self.spark.read \
            .format("parquet") \
            .load(get_mock_table_path(self.table)) \
            .orderBy(F.col("id").asc(), F.col("created_at").asc())
        return self

    def add_ncol_column(self) -> 'BronzeDataFrameDataGenerator':
        self.df = self.df.withColumn("ncol", F.lit(None).cast(T.StringType()))
        return self

    def remove_ncol_column(self) -> 'BronzeDataFrameDataGenerator':
        if "ncol" in self.df.columns:
            self.df = self.df.drop("ncol")
        return self

    def add_records(self, records: list[BronzeDataFrameRecord]) -> 'BronzeDataFrameDataGenerator':
        if "ncol" in self.df.schema.fieldNames():
            new_data = [
                (record.id, record.name, record.created_at, record.updated_at, record.ncol)
                for record in records
            ]
        else:
            new_data = [
                (record.id, record.name, record.created_at, record.updated_at)
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
                    F.when(F.col("id") == record.id, record.updated_at)
                    .otherwise(F.col("updated_at"))
                )
            if "ncol" in self.df.schema.fieldNames():
                self.df = self.df \
                    .withColumn(
                        "ncol",
                        F.when(F.col("id") == record.id, record.ncol)
                        .otherwise(F.col("ncol"))
                    )
        return self

    def delete_records(self, ids: list[int]) -> 'BronzeDataFrameDataGenerator':
        self.df = self.df.filter(~F.col("id").isin(ids))
        return self
