from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Optional


class Predictor(ABC):
    def __init__(self):
        self.model = None

    @staticmethod
    def get_extension(file_path: str) -> str:
        """Retrieves the file extension of a path.

        Parameters
        ----------
        file_path : str
            File path to get extension of.

        Returns
        -------
        str
            Extension of file_path.
        """
        return os.path.splitext(file_path)[1]

    @staticmethod
    @abstractmethod
    def is_compatible(file_path: str) -> bool:
        """Returns whether a file is compatible with the predictor.

        Parameters
        ----------
        file_path : str
            Path to file to check compatibility with.

        Returns
        -------
        bool
            Whether file_path is compatible with the predictor.
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def train_model(data_path: Optional[str], save_path: Optional[str]) -> None:
        """Trains and saves a predictor model.

        Parameters
        ----------
        data_path : str
            Path to data to train on.
        save_path : str
            Path to save model.
        """
        raise NotImplementedError

    @abstractmethod
    def load_model(self, load_path: Optional[str]) -> None:
        """Loads model to class.

        Parameters
        ----------
        load_path : str
            Path to predictor model to load.
        """
        raise NotImplementedError

    @abstractmethod
    def predict(self, file_path: str, file_size: float) -> float:
        """Predicts the size of decompressed data.

        Parameters
        ----------
        file_path : str
            Path of compressed file to predict on.
        file_size : float
            Size of compressed file to predict on.

        Returns
        -------
        float
            Prediction of decompressed file size.
        """
        raise NotImplementedError
