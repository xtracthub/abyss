from predictors.predictor import Predictor
from predictors.tar_predictor import TarPredictor
from predictors.gzip_predictor import GZipPredictor

file_predictor_mapping = {
    ".tar": TarPredictor,
    ".gz": GZipPredictor
}


def get_predictor(file_path):
    extension = Predictor.get_extension(file_path)

    try:
        return file_predictor_mapping[extension]
    except KeyError:
        raise ValueError(f"{extension} does not have a compatible predictor")
