import uuid

REQUIRED_WORKER_PARAMETERS = [
    ("globus_eid", str),
    ("funcx_eid", str),
    ("max_available_space", int),
    ("transfer_dir", str),
    ("decompress_dir", str)
]


class Worker:
    def __init__(self, globus_eid: str, funcx_eid: str,
                 transfer_dir: str, decompress_dir: str,
                 max_available_space: int):
        """Class for holding worker information.

        Parameters
        ----------
        globus_eid : str
            Globus endpoint ID located at worker.
        funcx_eid : str
            FuncX endpoint ID located at worker.
        transfer_dir : str
            Directory to transfer files to.
        decompress_dir : str
            Directory to decompress files to.
        max_available_space : int
            Maximum available space on worker.
        """
        self.globus_eid = globus_eid
        self.funcx_eid = funcx_eid
        self.transfer_dir = transfer_dir
        self.decompress_dir = decompress_dir
        self.max_available_space = max_available_space
        self.curr_available_space = max_available_space
        self.worker_id = str(uuid.uuid4())

    @staticmethod
    def validate_dict_params(worker_params: dict) -> None:
        """Ensures dictionary of worker parameters contains
        necessary parameters.

        Parameters
        ----------
        worker_params : dict
            Dictionary containing parameters for Worker object.

        Returns
        -------
            Returns None if parameters are valid, raises error if
            invalid.
        """
        try:
            for parameter_name, parameter_type in REQUIRED_WORKER_PARAMETERS:
                parameter = worker_params[parameter_name]
                assert isinstance(parameter, parameter_type)
        except AssertionError:
            raise ValueError(
                f"Worker parameter {parameter_name} is not of type {parameter_type}")
        except KeyError:
            raise ValueError(
                f"Worker parameter {parameter_name} not found")

    @staticmethod
    def from_dict(worker_params: dict):
        """Factory method for creating Worker objects from dictionary.

        Parameters
        ----------
        worker_params : dict
            Valid dictionary with worker parameters.

        Returns
        -------
        worker : Worker
            Worker object created from worker_params.
        """
        Worker.validate_dict_params(worker_params)

        worker = Worker(worker_params["globus_eid"],
                        worker_params["funcx_eid"],
                        worker_params["transfer_dir"],
                        worker_params["decompress_dir"],
                        worker_params["max_available_space"])
        return worker

