from enum import Enum


REQUIRED_JOB_PARAMETERS = [
    ("file_path", str),
    ("compressed_size", int)
]


class JobStatus(Enum):
    UNPREDICTED = "UNPREDICTED"
    PREDICTED = "PREDICTED"
    SCHEDULED = "SCHEDULED"
    PREFETCHING = "PREFETCHING"
    PREFETCHED = "PREFETCHED"
    DECOMPRESSING = "DECOMPRESSING"
    DECOMPRESSED = "DECOMPRESSED"
    CRAWLING = "CRAWLING"
    CONSOLIDATING = "CONSOLIDATING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class Job:
    def __init__(self, file_path=None, compressed_size=0,
                 decompressed_size=0, worker_id=None, transfer_path=None,
                 decompress_path=None, funcx_decompress_id=None,
                 funcx_crawl_id=None, status=JobStatus.UNPREDICTED):
        self.file_path = file_path
        self.compressed_size = compressed_size
        self.decompressed_size = decompressed_size
        self.total_size = compressed_size + decompressed_size
        self.worker_id = worker_id
        self.transfer_path = transfer_path
        self.decompress_path = decompress_path
        self.funcx_decompress_id = funcx_decompress_id
        self.funcx_crawl_id = funcx_crawl_id
        self.status = status

    @staticmethod
    def validate_dict_params(job_params: dict) -> None:
        """Ensures dictionary of job parameters contains
        necessary parameters.

        Parameters
        ----------
        job_params : dict
            Dictionary containing parameters for Worker object.

        Returns
        -------
            Returns None if parameters are valid, raises error if
            invalid.
        """
        try:
            for parameter_name, parameter_type in REQUIRED_JOB_PARAMETERS:
                parameter = job_params[parameter_name]
                assert isinstance(parameter, parameter_type)
        except AssertionError:
            raise ValueError(
                f"Job parameter {parameter_name} is not of type {parameter_type}")
        except KeyError:
            raise ValueError(
                f"Job parameter {parameter_name} not found")

    @staticmethod
    def from_dict(job_params: dict):
        """Factory method for creating Job objects from dictionary.

        Parameters
        ----------
        job_params : dict
            Valid dictionary with job parameters.

        Returns
        -------
        job : Job
            Job object created from job_params.
        """
        Job.validate_dict_params(job_params)

        job = Job(**job_params)
        return job






