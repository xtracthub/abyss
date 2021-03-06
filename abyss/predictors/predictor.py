from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Optional


class Predictor(ABC):
    def __init__(self):
        """Predictor for decompressed file size."""
        self.predictor_model = None
        self.repredictor_model = None

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
    def train_models(data_path: Optional[str],
                     predictor_save_path: Optional[str],
                     repredictor_save_path: Optional[str]) -> None:
        """Trains and saves a predictor and repredictor model. The
        predictor model takes the compressed size of a file and attempts
        to predict the decompressed size of the file. The repredictor
        model takes an underestimated decompressed size prediction and
        attempts to repredict its size.

        Parameters
        ----------
        data_path : str
            Path to data to train on.
        predictor_save_path : str
            Path to save predictor model.
        repredictor_save_path: str
            Path to save repredictor model.

        Returns
        ----------
        None
        """
        raise NotImplementedError

    @abstractmethod
    def load_models(self, predictor_model_path: Optional[str],
                    repredictor_model_path: Optional[str]) -> None:
        """Loads model to class.

        Parameters
        ----------
        predictor_model_path : str
            Path to predictor model to load.
        repredictor_model_path : str
            Path to repredictor model to load.

        Returns
        ----------
        None
        """
        raise NotImplementedError

    @abstractmethod
    def predict(self, file_path: str, file_size: int) -> int:
        """Predicts the size of decompressed data.

        Parameters
        ----------
        file_path : str
            Path of compressed file to predict on.
        file_size : int
            Size of compressed file to predict on.

        Returns
        -------
        int
            Prediction of decompressed file size.
        """
        raise NotImplementedError

    @abstractmethod
    def repredict(self, decompressed_size: int) -> int:
        """Creates new prediction for decompressed size given a previous
        decompressed size prediction.

        Parameters
        ----------
        decompressed_size : int
            Previous decompressed size prediction to repredict.

        Returns
        -------
        New prediction for decompressed size.

        """
        raise NotImplementedError

