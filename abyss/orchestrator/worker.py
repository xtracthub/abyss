from typing import Dict


REQUIRED_WORKER_PARAMETERS = [
    ("globus_eid", str),
    ("funcx_eid", str),
    ("available_space", int),
    ("transfer_dir", str),
    ("decompress_dir", str)
]


class Worker:
    def __init__(self, globus_eid: str, funcx_eid: str,
                 transfer_dir: str, decompress_dir: str,
                 available_space: int):
        """Class for holding worker information.

        Parameters
        ----------
        globus_eid : str
            Globus endpoint ID located at worker.
        funcx_eid : str
            FuncX endpoint ID located at worker.
        available_space : int
            Space available on worker.
        """
        self.globus_eid = globus_eid
        self.funcx_eid = funcx_eid
        self.transfer_dir = transfer_dir
        self.decompress_dir = decompress_dir
        self.available_space = available_space
        self.worker_id = self.funcx_eid

    @staticmethod
    def validate_dict_params(worker_params: Dict) -> None:
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
                f"Worker parameter parameter {parameter_name} not found")

    @staticmethod
    def from_dict(worker_params: Dict):
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
                        worker_params["available_space"])
        return worker

