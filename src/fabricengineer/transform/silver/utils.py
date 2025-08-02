from uuid import uuid4
from dataclasses import dataclass
from pyspark.sql import types as T, functions as F
from fabricengineer.transform.lakehouse import LakehouseTable


@F.udf(returnType=T.StringType())
def generate_uuid():
    """Generiert eine UUID4"""
    return str(uuid4())


@dataclass(frozen=True)
class ConstantColumn:
    """Class for adding a column with constant value to etl"""
    name: str
    value: str
    part_of_nk: bool = False

    def __post_init__(self):
        """
        Nach initialisierung wird der name in UPPERCASE umgewandelt.
        """
        object.__setattr__(self, "name", self.name.upper())


def get_mock_table_path(table: LakehouseTable) -> str:
    if table is None:
        raise ValueError("Table is not initialized.")
    table_path = table.table_path.replace(".", "/")
    full_table_path = f"tmp/lakehouse/{table_path}"
    return full_table_path
