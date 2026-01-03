from abc import ABC, abstractmethod


class BaseExporter(ABC):
    @abstractmethod
    def process(self, *args: tuple, **kwargs: dict) -> None: ...
