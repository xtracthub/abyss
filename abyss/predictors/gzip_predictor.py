import logging
import math
import os
import pickle as pkl
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from abyss.definitions import ROOT_DIR
from abyss.predictors.predictor import Predictor

logger = logging.getLogger(__name__)


class GZipPredictor(Predictor):
    def __init__(self):
        """Predictor for .gz files."""
        super().__init__()

    @staticmethod
    def is_compatible(file_path: str) -> bool:
        """Returns whether a file is compatible with gzip predictor.

        Parameters
        ----------
        file_path : str
            Path to file to check compatibility with.

        Returns
        -------
        bool
            Whether file_path is compatible with gzip predictor.
        """
        return Predictor.get_extension(file_path) == ".gz"

    @staticmethod
    def train_model(data_path: Optional[str] = "../../data/gzip_decompression_results.csv",
                    save_path: Optional[str] = "gzip_model.pkl") -> None:
        """Trains and saves a predictor model.

        Parameters
        ----------
        data_path : str
            Path to data to train on.
        save_path : str
            Path to save model.
        """
        data_df = pd.read_csv(data_path)

        x = np.array(data_df.compressed_size)
        y = np.array(data_df.decompressed_size)

        X = x.reshape(-1, 1)

        model = LinearRegression()
        model.fit(X, y)

        with open(save_path, "wb") as f:
            pkl.dump(model, f)

    def load_model(self, load_path: Optional[str] = os.path.join(ROOT_DIR,
                                                                 "predictors/gzip_model.pkl")) -> None:
        """Loads model to class.

        Parameters
        ----------
        load_path : str
            Path to predictor model to load.
        """
        with open(load_path, "rb") as f:
            self.model = pkl.load(f)

        logger.info(f"LOADED {load_path} as model")

    def predict(self, file_path: str, file_size: float) -> int:
        """Predicts the size of decompressed .gz file.

        Parameters
        ----------
        file_path : str
            Path of compressed file to predict on.
        file_size : float
            Size of compressed file to predict on.

        Returns
        -------
        float
            Prediction of decompressed .gz file size.
        """
        if not self.model:
            raise ValueError("Model must be loaded before running predictions.")

        x = np.array([file_size]).reshape(1, -1)

        decompressed_size = int(math.ceil(self.model.predict(x)[0]))

        logger.info(f"{file_path} DECOMPRESSED SIZE: {decompressed_size}B")

        return decompressed_size


if __name__ == "__main__":
    pass