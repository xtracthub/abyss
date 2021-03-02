import os
from abc import ABC, abstractmethod


class Crawler(ABC):
    @abstractmethod
    def crawl(self) -> dict:
        """Method for starting crawls.

        Returns
        -------
        dict
            Crawl metadata.
        """
        raise NotImplementedError

    @staticmethod
    def get_extension(file_path: str) -> str:
        """Returns the extension of a file path.

        Parameters
        ----------
        file_path : str
            File path to get extension of.

        Returns
        -------
        extension : str
            Extension of filepath.
        """
        extension = os.path.splitext(file_path)[1]

        return extension