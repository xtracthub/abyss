import logging
import math
import os
import pickle as pkl

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from abyss.definitions import ROOT_DIR
from abyss.predictors import Predictor

logger = logging.getLogger(__name__)


class ZipPredictor(Predictor):
    def __init__(self):
        """Predictor for .zip files."""
        super().__init__()

    @staticmethod
    def is_compatible(file_path: str) -> bool:
        """Returns whether a file is compatible with tar predictor.

        Parameters
        ----------
        file_path : str
            Path to file to check compatibility with.

        Returns
        -------
        bool
            Whether file_path is compatible with tar predictor.
        """
        return Predictor.get_extension(file_path) == ".zip"

    @staticmethod
    def train_model(data_path=os.path.join(ROOT_DIR, "../data/zip_decompression_results.csv"),
                    save_path=os.path.join(ROOT_DIR, "predictors/models/zip_model.pkl")) -> None:
        """Trains and saves a linear regression model. Additionally adds
        95th percentile error for error correction.

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

        model = LinearRegression(fit_intercept=False)
        model.fit(X, y)

        error = model.predict(X) - y
        percentile_error = np.quantile(error, 0.95)

        model.intercept_ = percentile_error

        with open(save_path, "wb") as f:
            pkl.dump(model, f)

    def load_model(self, load_path=os.path.join(ROOT_DIR, "predictors/models/zip_model.pkl")) -> None:
        """Loads model to class.

        Parameters
        ----------
        load_path : str
            Path to predictor model to load.
        """
        with open(load_path, "rb") as f:
            self.model = pkl.load(f)

        logger.info(f"LOADED {load_path} as model")

    def predict(self, file_path: str, file_size: int) -> int:
        """Predicts the size of decompressed .zip file.

        Parameters
        ----------
        file_path : str
            Path of compressed file to predict on.
        file_size : int
            Size of compressed file to predict on.

        Returns
        -------
        int
            Prediction of decompressed .zip file size.
        """
        if not self.model:
            raise ValueError("Model must be loaded before running predictions.")

        x = np.array([file_size]).reshape(1, -1)

        decompressed_size = int(math.ceil(self.model.predict(x)[0]))

        logger.info(f"{file_path} DECOMPRESSED SIZE: {decompressed_size}B")

        return decompressed_size


if __name__ == "__main__":
    ZipPredictor.train_model()