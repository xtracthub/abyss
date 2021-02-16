from abc import ABC, abstractmethod
from queue import Queue
from typing import Dict, List
from abyss.orchestrator.worker import Worker


REQUIRED_JOB_PARAMETERS = {
    "file_path": str,
    "decompressed_size": int
}


class Scheduler(ABC):
    def __init__(self, workers: List[Worker], jobs: List[Dict]):
        """Base class for creating batchers. Schedulers take in worker
        information (globus_eid, funcx_eid, space allocation in bytes)
        and an initial list of jobs (path, estimated decompressed size)
        to place into a queues for each worker.

        """
        self.workers = workers
        self.jobs = jobs

        self.validate_jobs(self.jobs)

        self.failed_jobs = [job for job in self.jobs if self._is_failed_job(job)]
        self.worker_queues = dict()

        for worker in workers:
            self.worker_queues[worker.worker_id] = Queue()


    @staticmethod
    def validate_jobs(jobs):
        try:
            for job in jobs:
                for parameter_name, parameter_type in REQUIRED_JOB_PARAMETERS.items():
                    parameter = job[parameter_name]
                    assert isinstance(parameter, parameter_type)
        except AssertionError:
            raise ValueError(f"Parameter {parameter_name} is not of type {parameter_type}")
        except KeyError:
            raise ValueError(f"Required parameter {parameter_name} not found")

    @abstractmethod
    def _schedule(self):
        raise NotImplementedError

    def _is_failed_job(self, job: Dict) -> bool:
        """Determines whether a job can't be scheduled based on job size
        and allocated space of workers.

        Parameters
        ----------
        job : dict
            Dictionary with file path and size of decompressed file.

        Returns
        -------
        bool
            Whether workers have enough space to process job.
        """
        for worker in self.workers:
            if worker.available_space >= job["decompressed_size"]:
                return True

        return False