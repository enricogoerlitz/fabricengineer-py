def to_spark_sql(sql: str) -> str:
    return sql \
            .replace("[", "`") \
            .replace("]", "`")
