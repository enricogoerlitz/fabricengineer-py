# FabricEngineer Package
[![CI](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/ci.yml/badge.svg)](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/ci.yml)
[![CD](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/release.yml/badge.svg)](https://github.com/enricogoerlitz/fabricengineer-py/actions/workflows/release.yml)

## Description

**FabricEngineer** is a comprehensive Python package designed specifically for Microsoft Fabric developers to streamline data transformation workflows and automate complex ETL processes. This package provides enterprise-grade solutions for building robust data pipelines with minimal boilerplate code. In addition, FabricEngineer enables environment-as-code for Microsoft Fabric: create and manage Lakehouses, Warehouses, Variable Libraries, Notebooks, and Data Pipelines programmatically via the Fabric API. Using a template notebook, you can define standardized workspaces and deploy them either directly in Fabric or through your CI/CD pipeline.

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

🏗️ **Environment-as-Code for Microsoft Fabric**
- **Programmatic Provisioning**: Create Lakehouses, Warehouses, Variable Libraries, Notebooks, and Data Pipelines via the Fabric API
- **Workspace Templating**: Define standard workspaces with a parameterized template notebook
- **Flexible Deployment**: Deploy directly in Microsoft Fabric or via CI/CD (e.g., GitHub Actions or Azure DevOps)
- **Repeatable Setups**: Consistent, code-driven environments with minimal boilerplate

🔧 **Advanced Data Engineering Features**
- **Configurable Transformations**: Flexible transformation pipelines with custom business logic
- **Data Quality Controls**: Built-in validation and data quality checks
- **Performance Optimization**: Broadcast joins, partition strategies, and optimized query patterns
- **Comprehensive Logging**: Integrated logging and performance monitoring with

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
from fabricengineer.transform import SilverIngestionInsertOnlyService


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
etl = SilverIngestionInsertOnlyService()
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
timer.stop().log()
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
timer.stop().log()
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

### Environment-as-Code

#### Manage Lakehouse

```python
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from fabricengineer.api.fabric.workspace.workspace import Workspace
from fabricengineer.api.fabric.workspace.items import Lakehouse


workspace = Workspace.get_by_name("<WORKSPACE_NAME")
workspace_id = workspace.item.id

lakehouse = Lakehouse(workspace_id, name="LakehouseName")

# Static methods
lakehouse_by_id = Lakehouse.get_by_id(workspace_id, id="LAKEHOUSE_ID")
lakehouse_by_name = Lakehouse.get_by_name(workspace_id, name="LAKEHOUSE_NAME")
lakehouses = Lakehouse.list(workspace_id)

# Create lakehouse
lakehouse.create()
lakehouse.create_if_not_exists()  # Save creation

# Update lakehouse
lakehouse.update(description="Updated description")

# Fetch current api data
lakehouse.fetch()

# Check exists
if lakehouse.exists():
    pass

# Delete
lakehouse.delete()

# API Fields
id = lakehouse.item.api.id
workspace_id = lakehouse.item.api.workspaceId
display_name = lakehouse.item.api.displayName
other = lakehouse.item.api.*

# Setted fields
fields: dict[str, Any] = lakehouse.item.fields

```

#### Manage WorkspaceFolder

```python
from fabricengineer.api.fabric.workspace.workspace import Workspace
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder

workspace = Workspace.get_by_name("<WORKSPACE_NAME>")
workspace_id = workspace.item.id

# Create folders
root_folder = WorkspaceFolder(workspace_id, name="RootFolder")
root_folder.create_if_not_exists()

sub_folder = WorkspaceFolder(workspace_id, name="SubFolder", parent_folder=root_folder)
sub_folder.create_if_not_exists()

# Static methods
folder_by_id = WorkspaceFolder.get_by_id(workspace_id, id="FOLDER_ID")
folder_by_name = WorkspaceFolder.get_by_name(workspace_id, name="RootFolder")
folders = WorkspaceFolder.list(workspace_id)

# Update (rename)
root_folder.update(displayName="RootFolderRenamed")

# Fetch current api data
root_folder.fetch()

# Check exists
if root_folder.exists():
    pass

# Delete
sub_folder.delete()

# API Fields
id = root_folder.item.api.id
workspace_id = root_folder.item.api.workspaceId
display_name = root_folder.item.api.displayName
parent_folder_id = root_folder.item.api.parentFolderId

# Set fields
fields: dict[str, Any] = root_folder.item.fields
```

#### Manage Warehouse

```python
from fabricengineer.api.fabric.workspace.workspace import Workspace
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from fabricengineer.api.fabric.workspace.items import Warehouse

workspace = Workspace.get_by_name("<WORKSPACE_NAME>")
workspace_id = workspace.item.id

folder = WorkspaceFolder(workspace_id, name="Warehouses")
folder.create_if_not_exists()

warehouse = Warehouse(
    workspace_id=workspace_id,
    name="WarehouseName",
    description="Description",
    folder=folder,
    collation_type="Latin1_General_100_BIN2_UTF8"
)

# Static methods
warehouse_by_id = Warehouse.get_by_id(workspace_id, id="WAREHOUSE_ID")
warehouse_by_name = Warehouse.get_by_name(workspace_id, name="WarehouseName")
warehouses = Warehouse.list(workspace_id)

# Create
warehouse.create()
warehouse.create_if_not_exists()

# Update
warehouse.update(description="Updated description")

# Fetch
warehouse.fetch()

# Exists
if warehouse.exists():
    pass

# Delete
warehouse.delete()

# API Fields
id = warehouse.item.api.id
workspace_id = warehouse.item.api.workspaceId
display_name = warehouse.item.api.displayName
connection_string = warehouse.item.api.properties.connectionString
collation_type = warehouse.item.api.properties.collationType

# Set fields
fields: dict[str, Any] = warehouse.item.fields
```

#### Manage Notebook

```python
from fabricengineer.api.fabric.workspace.workspace import Workspace
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from fabricengineer.api.fabric.workspace.items import (
    Notebook, IPYNBNotebookDefinition, CopyFabricNotebookDefinition
)

workspace = Workspace.get_by_name("<WORKSPACE_NAME>")
workspace_id = workspace.item.id

folder = WorkspaceFolder(workspace_id, name="Notebooks")
folder.create_if_not_exists()

# Empty notebook
notebook = Notebook(
    workspace_id=workspace_id,
    name="NotebookName",
    description="Description",
    folder=folder
)

# From .ipynb file
ipynb_def = IPYNBNotebookDefinition(ipynb_path="/path/to/notebook.ipynb")
notebook_from_ipynb = Notebook(
    workspace_id=workspace_id,
    name="NotebookFromIpynb",
    description="Description",
    definition=ipynb_def,
    folder=folder
)

# From copy
copy_def = CopyFabricNotebookDefinition("<SOURCE_WORKSPACE_ID>", "<SOURCE_NOTEBOOK_ID>")
notebook_from_copy = Notebook(
    workspace_id=workspace_id,
    name="NotebookFromCopy",
    description="Description",
    definition=copy_def,
    folder=folder
)

# Static methods
nb_by_id = Notebook.get_by_id(workspace_id, id="NOTEBOOK_ID")
nb_by_name = Notebook.get_by_name(workspace_id, name="NotebookName")
notebooks = Notebook.list(workspace_id)

# Create
notebook.create_if_not_exists()

# Update
notebook.update(description="Updated description")

# Fetch
notebook.fetch()

# Exists
if notebook.exists():
    pass

# Delete
notebook.delete()

# API Fields
id = notebook.item.api.id
workspace_id = notebook.item.api.workspaceId
display_name = notebook.item.api.displayName

# Set fields
fields: dict[str, Any] = notebook.item.fields
```

#### Manage DataPipeline

```python
from fabricengineer.api.fabric.workspace.workspace import Workspace
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from fabricengineer.api.fabric.workspace.items import (
    DataPipeline, ZIPDataPipelineDefinition, CopyDataPipelineDefinition
)

workspace = Workspace.get_by_name("<WORKSPACE_NAME>")
workspace_id = workspace.item.id

folder = WorkspaceFolder(workspace_id, name="Pipelines")
folder.create_if_not_exists()

# Empty pipeline
pipeline = DataPipeline(
    workspace_id=workspace_id,
    name="PipelineName",
    description="Description",
    folder=folder
)

# From ZIP definition
zip_def = ZIPDataPipelineDefinition(zip_path="/path/to/pipeline.zip")
pipeline_from_zip = DataPipeline(
    workspace_id=workspace_id,
    name="PipelineFromZip",
    description="Description",
    definition=zip_def,
    folder=folder
)

# From copy
copy_def = CopyDataPipelineDefinition("<SOURCE_WORKSPACE_ID>", "<SOURCE_PIPELINE_ID>")
pipeline_from_copy = DataPipeline(
    workspace_id=workspace_id,
    name="PipelineFromCopy",
    description="Description",
    definition=copy_def,
    folder=folder
)

# Static methods
dp_by_id = DataPipeline.get_by_id(workspace_id, id="PIPELINE_ID")
dp_by_name = DataPipeline.get_by_name(workspace_id, name="PipelineName")
pipelines = DataPipeline.list(workspace_id)

# Create
pipeline.create_if_not_exists()

# Update
pipeline.update(description="Updated description")

# Fetch
pipeline.fetch()

# Exists
if pipeline.exists():
    pass

# Delete
pipeline.delete()

# API Fields
id = pipeline.item.api.id
workspace_id = pipeline.item.api.workspaceId
display_name = pipeline.item.api.displayName

# Set fields
fields: dict[str, Any] = pipeline.item.fields
```

#### Manage VariableLibrary

```python
from fabricengineer.api.fabric.workspace.workspace import Workspace
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from fabricengineer.api.fabric.workspace.items import (
    VariableLibrary, VariableLibraryDefinition, VariableLibraryVariable
)

workspace = Workspace.get_by_name("<WORKSPACE_NAME>")
workspace_id = workspace.item.id

folder = WorkspaceFolder(workspace_id, name="Variables")
folder.create_if_not_exists()

definition = VariableLibraryDefinition(
    ["TEST", "PROD"],
    VariableLibraryVariable(name="ApiUrl", note="", type="String", value="https://api.test"),
    VariableLibraryVariable(name="ApiKey", note="", type="String", value="secret")
)

varlib = VariableLibrary(
    workspace_id=workspace_id,
    name="VariableLibrary",
    description="Description",
    folder=folder,
    definition=definition
)

# Static methods
vl_by_id = VariableLibrary.get_by_id(workspace_id, id="VARIABLE_LIBRARY_ID")
vl_by_name = VariableLibrary.get_by_name(workspace_id, name="VariableLibrary")
varlibs = VariableLibrary.list(workspace_id)

# Create
varlib.create_if_not_exists()

# Update
varlib.update(description="Updated description")

# Fetch
varlib.fetch()

# Exists
if varlib.exists():
    pass

# Delete
varlib.delete()

# API Fields
id = varlib.item.api.id
workspace_id = varlib.item.api.workspaceId
display_name = varlib.item.api.displayName
active_value_set = varlib.item.api.properties.activeValueSetName

# Set fields
fields: dict[str, Any] = varlib.item.fields
```

#### Manage Workspace

```python
from fabricengineer.api.fabric.workspace.workspace import Workspace

# Create
ws = Workspace(
    name="MyWorkspace",
    description="New Workspace",
    capacity_id="<CAPACITY_ID>"  # Optional
)
ws.create()
ws.create_if_not_exists()

# Static methods
ws_by_id = Workspace.get_by_id("WORKSPACE_ID")
ws_by_name = Workspace.get_by_name("MyWorkspace")
workspaces = Workspace.list()

# Update
ws.update(description="Updated description")

# Fetch
ws.fetch()

# Exists
if ws.exists():
    pass

# Delete
ws.delete()

# API Fields
id = ws.item.api.id
display_name = ws.item.api.displayName
description = ws.item.api.description
capacity_id = ws.item.api.capacityId
capacity_region = ws.item.api.capacityRegion
one_lake_blob = ws.item.api.oneLakeEndpoints.blobEndpoint
one_lake_dfs = ws.item.api.oneLakeEndpoints.dfsEndpoint

# Set fields
fields: dict[str, Any] = ws.item.fields
```

#### Use WorkspaceItemCreationPipeline

```python
from fabricengineer.api.fabric.workspace.folder.folder import WorkspaceFolder
from fabricengineer.api.fabric.workspace.items import (
    DataPipeline, CopyDataPipelineDefinition, ZIPDataPipelineDefinition,
    Notebook, CopyFabricNotebookDefinition, IPYNBNotebookDefinition,
    VariableLibrary, VariableLibraryProperties, VariableLibraryDefinition
    Warehouse,
    Lakehouse
)
from fabricengineer.api.fabric.workspace.create_pipeline import (
    WorkspaceItemCreationPipeline,
    PipelineItemStatus
)


workspace_id = "<WORKSPACE_ID>"

# Folders
root_folder = WorkspaceFolder(workspace_id, name="RootFolder")
sub_folder = WorkspaceFolder(workspace_id, name="SubFolder", parent_folder=root_folder)

# DataPipeline
data_pipeline_empty = DataPipeline(
    workspace_id=workspace_id,
    name=name,
    description="Description",
    folder=None
)

zip_path = "/path/to/pipeline.zip"
zip_defintion = ZIPDataPipelineDefinition(zip_path=path)
data_pipeline_from_zip = DataPipeline(
    workspace_id=workspace_id,
    name=name,
    description="Description",
    definition=zip_definition,
    folder=root_folder
)

copy_data_pipeline_definition = CopyDataPipelineDefinition("<WORKSPACE_ID>", "<PIPELINE_ID>")
data_pipeline_copy = DataPipeline(
    workspace_id=workspace_id,
    name=name,
    description="Description",
    definition=copy_data_pipeline_definition,
    folder=sub_folder
)

# Lakehouse
lakehouse = Lakehouse(
    workspace_id=workspace_id,
    name="LakehouseName",
    description="Description",
    folder=root_folder
)

# Notebook
notebook_empty = Notebook(
    workspace_id=workspace_id,
    name="Notebook",
    description="Description",
    folder=None
)

ipynb_path = "/path/to/notebook.ipynb"
ipynb_notebook_definition = IPYNBNotebookDefinition(ipynb_path=ipynb_path)
notebook_from_ipynb = Notebook(
    workspace_id=workspace_id,
    name="Notebook",
    description="Description",
    definition=ipynb_notebook_definition,
    folder=None
)

copy_notebook_definition = CopyFabricNotebookDefinition("<WORKSPACE_ID>", "<NOTEBOOK_ID>")
notebook_from_copy = Notebook(
    workspace_id=workspace_id,
    name="Notebook",
    description="Description",
    definition=copy_notebook_definition,
    folder=None
)

# VariableLibrary
varlib_definition = VariableLibraryDefinition(
    ["TEST", "PROD"],
    VariableLibraryVariable(
        name="Variable1",
        note="",
        type="String",
        value="blub-default"
    ),
    VariableLibraryVariable(
        name="Variable2",
        note="",
        type="String",
        value="blab-default"
    )
)

variable_library = VariableLibrary(
    workspace_id=WORKSPACE_ID,
    name="VariableLibrary",
    definition=definition
)

# Warehouse
warehouse = Warehouse(
    workspace_id=workspace_id,
    name="WarehouseName",
    description="Description",
    folder=root_folder
)

# Create and execute WorkspaceItemCreationPipeline
pipeline = WorkspaceItemCreationPipeline([
    root_folder,
    sub_folder,
    data_pipeline_empty,
    data_pipeline_from_zip,
    data_pipeline_copy,
    notebook_empty,
    notebook_from_ipynb,
    notebook_from_copy,
    variable_library,
    lakehouse,
    warehouse
])

result = pipeline.run(in_parallel=True)
print(result)
```

### Remote Module Import for Fabric Notebooks

Import specific package modules directly into your Fabric notebooks from GitHub releases:

```python
# Cell 1:
import requests

VERSION = "1.0.0"
url = f"https://raw.githubusercontent.com/enricogoerlitz/fabricengineer-py/refs/tags/{VERSION}/src/fabricengineer/import_module/import_module.py"
resp = requests.get(url)
code = resp.text

exec(code, globals())  # This provides the 'import_module' function
assert code.startswith("import requests")
assert "def import_module" in code

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

timer.stop().log()

# Custom fabricengineer logging
logger.info("Custom log message")
logger.error("Error occurred during processing")
```
