from typing import Dict


REQUIRED_PARAMETERS = [
            ("globus_source_eid", str),
            ("auth_token", str),
            ("transfer_token", str),
            ("compressed_files", list),
            ("grouper", str),
            ("worker_params", list)
        ]

REQUIRED_WORKER_PARAMETERS = [
    ("globus_dest_eid", str),
    ("funcx_eid", str),
    ("bytes_available", int)
]


class AbyssOrchestrator:
    def __init__(self):
        pass

    @staticmethod
    def validate_dict_params(orchestrator_params: Dict) -> None:
        """Ensures dictionary of orchestrator parameters contains
        necessary parameters.

        Parameters
        ----------
        orchestrator_params : dict
            Dictionary containing parameters for AbyssOrchestrator
            object.

        Returns
        -------
            Returns None if parameters are valid, raises error if
            invalid.
        """
        try:
            for parameter_name, parameter_type in REQUIRED_PARAMETERS:
                parameter = orchestrator_params[parameter_name]
                assert isinstance(parameter, parameter_type)
        except AssertionError:
            raise ValueError(f"Parameter {parameter_name} is not of type {parameter_type}")
        except KeyError:
            raise ValueError(f"Required parameter {parameter_name} not found")

        try:
            worker_params = orchestrator_params["worker_params"]

            for worker_param in worker_params:
                for parameter_name, parameter_type in REQUIRED_WORKER_PARAMETERS:
                    parameter = worker_param[parameter_name]
                    assert isinstance(parameter, parameter_type)
        except AssertionError:
            raise ValueError(f"Worker parameter {parameter_name} is not of type {parameter_type}")
        except KeyError:
            raise ValueError(f"Worker parameter parameter {parameter_name} not found")