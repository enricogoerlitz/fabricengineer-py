from abc import ABC, abstractmethod


class BaseSilverIngestionService(ABC):
    @abstractmethod
    def init(self, **kwargs): pass

    @abstractmethod
    def ingest(self, **kwargs): pass
