# FabricEngineer Package
[![CI](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/ci.yml/badge.svg)](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/ci.yml)
[![CD](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/release.yml/badge.svg)](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/release.yml)

## Description

**FabricEngineer** is a comprehensive Python package designed specifically for Microsoft Fabric developers to streamline data transformation workflows and automate complex ETL processes. This package provides enterprise-grade solutions for building robust data pipelines with minimal boilerplate code.

### Key Features

🚀 **Silver Layer Data Ingestion Services**
- **Insert-Only Pattern**: Efficient data ingestion with support for schema evolution and historization
- **SCD Type 2 (Slowly Changing Dimensions)**: Complete implementation of Type 2 SCD with automatic history tracking
- **Delta Load Support**: Optimized incremental data processing with broadcast join capabilities
- **Schema Evolution**: Automatic handling of schema changes with backward compatibility

📊 **Materialized Lake Views (MLV)**
- **Automated MLV Generation**: Create and manage materialized views with SQL generation
- **Schema-aware Operations**: Intelligent handling of schema changes and column evolution
- **Lakehouse Integration**: Seamless integration with Microsoft Fabric Lakehouse architecture

🔧 **Advanced Data Engineering Features**
- **Configurable Transformations**: Flexible transformation pipelines with custom business logic
- **Data Quality Controls**: Built-in validation and data quality checks
- **Performance Optimization**: Broadcast joins, partition strategies, and optimized query patterns
- **Comprehensive Logging**: Integrated logging and performance monitoring with TimeLogger

## Installation

```bash
pip install fabricengineer-py
```

## Quick Start Guide

### Prerequisites

- Microsoft Fabric workspace with Lakehouse
- PySpark environment
- Python 3.11+

## Usage Examples

### Silver Layer Data Ingestion

#### Insert-Only Pattern

The Insert-Only service is ideal for append-only scenarios where you need to track all changes while maintaining performance.

```python
from pyspark.sql import DataFrame, functions as F
from fabricengineer.logging import TimeLogger
from fabricengineer.transform.lakehouse import LakehouseTable
from fabricengineer.transform import SilverIngesationInsertOnly


def transform_projects(df: DataFrame, etl) -> DataFrame:
    df = df.withColumn("dtime", F.to_timestamp("dtime"))
    return df


def transform_all(df: DataFrame, etl) -> DataFrame:
    df = df.withColumn("data", F.lit("values"))
    return df


# Initialize performance monitoring
timer = TimeLogger()

# Define table-specific transformations
transformations = {
    "*": transform_all,             # Applied to all tables
    "projects": transform_projects  # Applied only to projects table
}

# Configure source and destination tables
source_table = LakehouseTable(
    lakehouse="BronzeLakehouse",
    schema="schema",
    table="projects"
)
destination_table = LakehouseTable(
    lakehouse="SilverLakehouse",
    schema=source_table.schema,
    table=source_table.table
)

# Initialize and configure the ETL service
etl = SilverIngestionInsertOnly()
etl.init(
    spark_=spark,
    notebookutils_=notebookutils,
    source_table=source_table,
    destination_table=destination_table,
    nk_columns=NK_COLUMNS,
    constant_columns=CONSTANT_COLUMNS,
    is_delta_load=IS_DELTA_LOAD,
    delta_load_use_broadcast=DELTA_LOAD_USE_BROADCAST,
    transformations=transformations,
    exclude_comparing_columns=EXCLUDE_COLUMNS_FROM_COMPARING,
    include_comparing_columns=INCLUDE_COLUMNS_AT_COMPARING,
    historize=HISTORIZE,
    partition_by_columns=PARTITION_BY_COLUMNS,
    df_bronze=None,
    create_historized_mlv=True
)


timer.start().log()
etl.run()
timer.end().log()
```

#### SCD Type 2 (Slowly Changing Dimensions)

The SCD2 service implements Type 2 Slowly Changing Dimensions with automatic history tracking and current record management.

```python
from pyspark.sql import DataFrame, functions as F
from fabricengineer.logging import TimeLogger
from fabricengineer.transform.lakehouse import LakehouseTable
from fabricengineer.transform import SilverIngestionSCD2Service


def transform_projects(df: DataFrame, etl) -> DataFrame:
    df = df.withColumn("dtime", F.to_timestamp("dtime"))
    return df


def transform_all(df: DataFrame, etl) -> DataFrame:
    df = df.withColumn("data", F.lit("values"))
    return df


# Initialize performance monitoring
timer = TimeLogger()

# Define table-specific transformations
transformations = {
    "*": transform_all,             # Applied to all tables
    "projects": transform_projects  # Applied only to projects table
}

# Configure source and destination tables
source_table = LakehouseTable(
    lakehouse="BronzeLakehouse",
    schema="schema",
    table="projects"
)
destination_table = LakehouseTable(
    lakehouse="SilverLakehouse",
    schema=source_table.schema,
    table=source_table.table
)

# Initialize and configure the ETL service
etl = SilverIngestionSCD2Service()
etl.init(
    spark_=spark,
    notebookutils_=notebookutils,
    source_table=source_table,
    destination_table=destination_table,
    nk_columns=NK_COLUMNS,
    constant_columns=CONSTANT_COLUMNS,
    is_delta_load=IS_DELTA_LOAD,
    delta_load_use_broadcast=DELTA_LOAD_USE_BROADCAST,
    transformations=transformations,
    exclude_comparing_columns=EXCLUDE_COLUMNS_FROM_COMPARING,
    include_comparing_columns=INCLUDE_COLUMNS_AT_COMPARING,
    historize=HISTORIZE,
    partition_by_columns=PARTITION_BY_COLUMNS,
    df_bronze=None
)


timer.start().log()
etl.run()
timer.end().log()
```

### Materialized Lake Views Management

**Prerequisites**

Configure a Utils Lakehouse as your default Lakehouse. The generated view SQL code will be saved as `.sql.txt` files in the lakehouse under `/Files/mlv/{lakehouse}/{schema}/{table}.sql.txt`.

```python
from fabricengineer.mlv import MaterializeLakeView

# Initialize the Materialized Lake View manager
mlv = MaterializedLakeView(
    lakehouse="SilverBusinessLakehouse",
    schema="schema",
    table="projects"
)
print(mlv.to_dict())

# Define your custom SQL query
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

# Create or replace the materialized view
result = mlv.create_or_replace(sql)
display(result)
```

### Remote Module Import for Fabric Notebooks

Import specific package modules directly into your Fabric notebooks from GitHub releases:

```python
# Cell 1:
import requests

VERSION = "0.1.0"
url = f"https://raw.githubusercontent.com/enricogoerlitz/fabricengineer-py/refs/tags/{VERSION}/src/fabricengineer/import_module/import_module.py"
resp = requests.get(url)
code = resp.text

exec(code, globals())  # This provides the 'import_module' function
assert code.startswith("import requests")

# Cell 2
mlv_module = import_module("transform.mlv", VERSION)
scd2_module = import_module("transform.silver.scd2", VERSION)
insertonly_module = import_module("transform.silver.insertonly", VERSION)

# Cell 3 - Use mlv module
exec(mlv_module, globals())  # Provides MaterializedLakeView class and mlv instance

mlv.init(
    lakehouse="SilverBusinessLakehouse",
    schema="schema",
    table="projects"
)
print(mlv.to_dict())

# Cell 4 - Use scd2 module
exec(scd2_module, globals())  # Provides an instantiated etl object

etl.init(...)
print(str(etl))

# Cell 5 - Use insertonly module
exec(insertonly_module, globals())  # Provides an instantiated etl object

etl.init(...)
print(str(etl))
```

## Advanced Features

### Performance Optimization

- **Broadcast Joins**: Automatically optimize small table joins
- **Partition Strategies**: Intelligent partitioning for better query performance
- **Schema Evolution**: Handle schema changes without breaking existing pipelines
- **Delta Load Processing**: Efficient incremental data processing

### Data Quality & Validation

- **Automatic Validation**: Built-in checks for data consistency and quality
- **Type Safety**: Comprehensive type annotations for better development experience
- **Error Handling**: Robust error handling and recovery mechanisms

### Monitoring & Logging

```python
from fabricengineer.logging import TimeLogger, logger

# Performance monitoring
timer = TimeLogger()
timer.start().log()

# Your ETL operations here
etl.run()

timer.end().log()

# Custom fabricengineer logging
logger.info("Custom log message")
logger.error("Error occurred during processing")
```
