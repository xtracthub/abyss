import os
from abc import ABC, abstractmethod


class Crawler(ABC):
    @abstractmethod
    def crawl(self):
        raise NotImplementedError

    @staticmethod
    def get_extension(file_path):
        """Returns the extension of a filepath.
        Parameter:
        filepath (str): Filepath to get extension of.
        Return:
        extension (str): Extension of filepath.
        """
        extension = os.path.splitext(file_path)[1]

        return extension