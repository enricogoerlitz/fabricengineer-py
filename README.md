# FabricEngineer Package
[![CI](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/ci.yml/badge.svg)](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/ci.yml)
[![CD](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/release.yml/badge.svg)](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/release.yml)

## Description

...

## Quickstart

### Run Silver Ingestion

#### SilverIngesationInsertOnly

```python
from pyspark.sql import DataFrame, functions as F

from fabricengineer.transform import (
    BaseSilverIngestion,
    SilverIngesationInsertOnly,
    BronzeTransformation
)
from fabricengineer.logging import TimeLogger


def transform_projects(
    df: DataFrame,
    etl: BaseSilverIngestion
) -> DataFrame:
    df = df.withColumn("dtime", F.to_timestamp("dtime"))
    return df


timer = TimeLogger()

transformations = [
    BronzeTransformation(table="projects", fn=transform_projects)
]

etl = SilverIngestionInsertOnly(
    spark=spark,
    df_bronze=None,
    src_lakehouse=SOURCE_LAKEHOUSE,
    src_schema=SOURCE_SCHEMA,
    src_tablename=SOURCE_TABLENAME,
    dist_lakehouse=DESTINATION_LAKEHOUSE,
    dist_schema=DESTINATION_SCHEMA,
    dist_tablename=DESTINATION_TABLENAME,
    nk_columns=NK_COLUMNS,
    constant_columns=CONSTANT_COLUMNS,
    is_delta_load=IS_DELTA_LOAD,
    delta_load_use_broadcast=DELTA_LOAD_USE_BROADCAST,
    transformations=TRANSFORMATIONS,
    exclude_comparing_columns=EXCLUDE_COLUMNS_FROM_COMPARING,
    include_comparing_columns=INCLUDE_COLUMNS_AT_COMPARING,
    historize=HISTORIZE,
    partition_by_columns=PARTITION_BY_COLUMNS,
    create_history_mlv=CREATE_HISTORY_MLV
)



timer.start().log()

etl.run()

timer.end().log()
```

#### SilverIngesationSCD2

```python
from pyspark.sql import DataFrame, functions as F

from fabricengineer.transform import (
    BaseSilverIngestion,
    SilverIngesationSCD2,
    BronzeTransformation
)
from fabricengineer.logging import TimeLogger


def transform_projects(
    df: DataFrame,
    etl: BaseSilverIngestion
) -> DataFrame:
    df = df.withColumn("dtime", F.to_timestamp("dtime"))
    return df


timer = TimeLogger()

transformations = [
    BronzeTransformation(table="projects", fn=transform_projects)
]

etl = SilverIngesationSCD2(
    spark=spark,
    df_bronze=None,
    src_lakehouse=SOURCE_LAKEHOUSE,
    src_schema=SOURCE_SCHEMA,
    src_tablename=SOURCE_TABLENAME,
    dist_lakehouse=DESTINATION_LAKEHOUSE,
    dist_schema=DESTINATION_SCHEMA,
    dist_tablename=DESTINATION_TABLENAME,
    nk_columns=NK_COLUMNS,
    constant_columns=CONSTANT_COLUMNS,
    is_delta_load=IS_DELTA_LOAD,
    delta_load_use_broadcast=DELTA_LOAD_USE_BROADCAST,
    transformations=TRANSFORMATIONS,
    exclude_comparing_columns=EXCLUDE_COLUMNS_FROM_COMPARING,
    include_comparing_columns=INCLUDE_COLUMNS_AT_COMPARING,
    historize=HISTORIZE,
    partition_by_columns=PARTITION_BY_COLUMNS
)


timer.start().print()

etl.run()

timer.end().print()



Eigenes Package: fabric-utils-py (SilverIngestionInsertOnly, MaterializedLakeView(lakehouse, schema, table_name, mode=CREATE | DROP_CREATE).execute(); ...

```

### Manage MaterializeLakeViews


**Create once**

```python
from fabricengineer.mlv import MaterializeLakeView


sql = """
SELECT
    p.id
    ,p.projectname
    ,p.budget
    ,u.name AS projectlead
FROM dbo.projects p
LEFT JOIN users u
ON p.projectlead_id = u.id
"""

mlv = MaterializeLakeView(sql, spark=spark)
mlv.create(mode=MLVMode.CREATE)  # Creates the MLV once
```

**Recreate MLV**

```python
mlv = MaterializeLakeView(sql, spark=spark)
mlv.recreate(mode=MLVMode.CREATE)  # Drops and Creates the MLV
```
