import numpy as np
import pandas as pd
import pickle as pkl
from predictors.predictor import Predictor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline


class TarPredictor(Predictor):
    def __init__(self):
        super().__init__()

    @staticmethod
    def is_compatible(file_path: str) -> bool:
        return Predictor.get_extension(file_path) == ".tar"

    @staticmethod
    def train_model(data_path: str, save_path: str) -> None:
        data_df = pd.read_csv(data_path)

        x = np.array(data_df.compressed_size)
        y = np.array(data_df.decompressed_size)

        degree = 3
        X = x.reshape(-1, 1)

        model = make_pipeline(PolynomialFeatures(degree), LinearRegression())
        model.fit(X, y)

        with open(save_path, "wb") as f:
            pkl.dump(model, f)

    def load_model(self, load_path="tar_model.pkl") -> None:
        with open(load_path, "rb") as f:
            self.model = pkl.load(f)

    def predict(self, file_path: str, file_size: float) -> float:
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
        if not self.model:
            raise ValueError("Model must be loaded before running predictions.")

        x = np.array([file_size]).reshape(1, -1)

        return self.model.predict(x)[0]


if __name__ == "__main__":
    TarPredictor.train_model("../data/tar_decompression_results.csv", "tar_model.pkl")
    p = TarPredictor()
    p.load_model("tar_model.pkl")
    print(p.predict("/blah/blah", 1000))
