from abc import ABC, abstractmethod
from typing import List


class Grouper(ABC):
    @abstractmethod
    def group(self, file_ls: List[str]):
        raise NotImplementedError
