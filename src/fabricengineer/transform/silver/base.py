from abc import ABC, abstractmethod
from dataclasses import dataclass


class BaseSilverIngestionService(ABC):
    @abstractmethod
    def init(self, **kwargs): pass

    @abstractmethod
    def ingest(self, **kwargs): pass


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


@dataclass
class LakehouseTable:
    lakehouse: str
    schema: str
    table: str

    @property
    def table_path(self) -> str:
        return f"{self.lakehouse}.{self.schema}.{self.table}"
