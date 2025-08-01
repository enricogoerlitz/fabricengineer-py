from dataclasses import dataclass


@dataclass
class LakehouseTable:
    lakehouse: str
    schema: str
    table: str

    @property
    def table_path(self) -> str:
        return f"{self.lakehouse}.{self.schema}.{self.table}"
