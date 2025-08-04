from abc import ABC, abstractmethod


# base.py


class BaseSilverIngestionService(ABC):
    @abstractmethod
    def init(self, **kwargs): pass

    @abstractmethod
    def ingest(self, **kwargs): pass
