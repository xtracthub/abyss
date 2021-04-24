import logging
import math
import os
import pickle as pkl
from typing import Optional

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
    def train_models(data_path=os.path.join(ROOT_DIR, "../data/zip_decompression_results.csv"),
                     predictor_save_path=os.path.join(ROOT_DIR, "predictors/models/zip_predictor_model.pkl"),
                     repredictor_save_path=os.path.join(ROOT_DIR, "predictors/models/zip_repredictor_model.pkl")) -> None:
        data_df = pd.read_csv(data_path)

        predictor_x = np.array(data_df.compressed_size)
        predictor_y = np.array(data_df.decompressed_size)

        predictor_X = predictor_x.reshape(-1, 1)

        predictor_model = LinearRegression(fit_intercept=False)
        predictor_model.fit(predictor_X, predictor_y)

        predictor_y_pred = predictor_model.predict(predictor_X)
        predictor_error = predictor_y_pred - predictor_y

        with open(predictor_save_path, "wb") as f:
            pkl.dump(predictor_model, f)

        repredictor_x = abs(predictor_y_pred[predictor_error < 0])
        repredictor_y = abs(predictor_error[predictor_error < 0])

        repredictor_X = repredictor_x.reshape(-1, 1)

        repredictor_model = LinearRegression(fit_intercept=False)
        repredictor_model.fit(repredictor_X, repredictor_y)

        with open(repredictor_save_path, "wb") as f:
            pkl.dump(repredictor_model, f)

    def load_models(self, predictor_model_path=os.path.join(ROOT_DIR, "predictors/models/zip_predictor_model.pkl"),
                    repredictor_model_path=os.path.join(ROOT_DIR, "predictors/models/zip_repredictor_model.pkl")) -> None:
        """Loads predictor and repredictor models to class.

        Parameters
        ----------
        predictor_model_path : str
            Path to predictor model to load.
        repredictor_model_path : str
            Path to repredictor model to load.
        """
        with open(predictor_model_path, "rb") as f:
            self.predictor_model = pkl.load(f)

        logger.info(f"Loaded {predictor_model_path} as predictor model")

        with open(repredictor_model_path, "rb") as f:
            self.repredictor_model = pkl.load(f)

        logger.info(f"Loaded {repredictor_model_path} as repredictor model")

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
        if not self.predictor_model:
            raise ValueError("Predictor model must be loaded before running predictions.")

        x = np.array([file_size]).reshape(1, -1)

        decompressed_size = int(math.ceil(self.predictor_model.predict(x)[0]))

        logger.info(f"{file_path} DECOMPRESSED SIZE: {decompressed_size}B")

        return decompressed_size

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
        if not self.repredictor_model:
            raise ValueError("Repredictor model must be loaded before running predictions.")

        x = np.array([decompressed_size]).reshape(1, -1)

        decompressed_size += int(math.ceil(self.repredictor_model.predict(x)[0]))

        return decompressed_size


if __name__ == "__main__":
    ZipPredictor.train_models()
