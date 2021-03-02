from abyss.predictors.gzip_predictor import GZipPredictor
from abyss.predictors.predictor import Predictor
from abyss.predictors.tar_predictor import TarPredictor

FILE_PREDICTOR_MAPPING = {
    ".tar": TarPredictor,
    ".gz": GZipPredictor
}


def get_predictor(file_path: str) -> Predictor:
    """Returns an appropriate predictor for a compressed file.

    Parameters
    ----------
    file_path : str
        File path to return predictor for.

    Returns
    -------
    Predictor
        File predictor.
    """
    extension = Predictor.get_extension(file_path)

    try:
        return FILE_PREDICTOR_MAPPING[extension]
    except KeyError:
        raise ValueError(f"{extension} does not have a compatible predictor")
