from abc import ABC, abstractmethod
from typing import Dict, List

from abyss.orchestrator.worker import Worker

REQUIRED_JOB_PARAMETERS = {
    "file_path": str,
    "decompressed_size": int
}


class Batcher(ABC):
    def __init__(self, workers: List[Worker], jobs: List[Dict]):
        """Base class for creating batchers. Batchers take in worker
        objects and an initial list of jobs and distributes jobs amongst
        workers such that the total size of jobs < available space on
        worker. An ideal batcher would minimize the maximum difference
        between the available space on worker and total size of jobs.

        Parameters
        ----------
        workers : list(Worker)
            List of Worker objects to batch jobs amongst.
        jobs : list(dict)
            List of jobs (dictionaries containing file_path and
            decompressed_size) to batch amongst workers.
        """
        self.workers = workers
        self.jobs = jobs
        self.worker_batches = dict()
        self.worker_dict = dict()
        self.failed_jobs = []

        for worker in workers:
            self.worker_batches[worker.worker_id] = []

        for worker in workers:
            self.worker_dict[worker.worker_id] = worker

    @staticmethod
    def validate_jobs(jobs: List[Dict]) -> None:
        """Ensures job dictionaries contain parameters of the correct type.

        Parameters
        ----------
        jobs : list(dict)
            List of jobs (dictionaries containing file_path and
            decompressed_size) to validate.

        Returns
        -------
        None
        """
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
    def batch_job(self, job: Dict) -> None:
        """Places job in queue to be batched.

        Parameters
        ----------
        job : dict
            Dictionary with file path and size of decompressed file.

        Returns
        -------
        None
        """
        raise NotImplementedError

    @abstractmethod
    def batch_jobs(self, jobs: List[Dict]) -> None:
        """Places batch of jobs in queue to be batched.

        Parameters
        ----------
        jobs : list(dict)
            List of dictionaries with file path and size of decompressed
            file.

        Returns
        -------
        None
        """
        raise NotImplementedError

    @abstractmethod
    def _batch(self):
        """Internal method for batching jobs."""
        raise NotImplementedError

    def _is_failed_job(self, job: Dict) -> bool:
        """Determines whether a job can't be batched based on job size
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
            if worker.max_available_space >= job["decompressed_size"]:
                return False

        return True

    def update_worker(self, worker: Worker) -> None:
        """Updates a worker. If the current available size of a worker
        is changed, then batches will be updated.

        Parameters
        ----------
        worker : Worker
            Worker to be updated in batcher.

        Returns
        -------
        None
        """
        try:
            self.worker_dict[worker.worker_id] = worker
            self._batch()
        except KeyError:
            raise ValueError(f"Worker {worker.worker_id} does not already exist")
