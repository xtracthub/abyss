from predictors.predictor import Predictor
from predictors.tar_predictor import TarPredictor
from predictors.gzip_predictor import GZipPredictor

FILE_PREDICTOR_MAPPING = {
    ".tar": TarPredictor,
    ".gz": GZipPredictor
}


def get_predictor(file_path):
    extension = Predictor.get_extension(file_path)

    try:
        return FILE_PREDICTOR_MAPPING[extension]
    except KeyError:
        raise ValueError(f"{extension} does not have a compatible predictor")
