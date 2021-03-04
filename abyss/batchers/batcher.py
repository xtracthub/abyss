from abc import ABC, abstractmethod
from typing import List

from abyss.orchestrator.job import Job
from abyss.orchestrator.worker import Worker

REQUIRED_JOB_PARAMETERS = {
    "file_path": str,
    "decompressed_size": int
}


class Batcher(ABC):
    def __init__(self, workers: List[Worker], jobs: List[Job]):
        """Base class for creating batchers. Batchers take in worker
        objects and an initial list of jobs and distributes jobs amongst
        workers such that the total size of jobs < available space on
        worker. An ideal batcher would minimize the maximum difference
        between the available space on worker and total size of jobs.

        Parameters
        ----------
        workers : list(Worker)
            List of Worker objects to batch jobs amongst.
        jobs : list(Job)
            List of Jobs to batch amongst workers.
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

    @abstractmethod
    def batch_job(self, job: Job) -> None:
        """Places job in queue to be batched.

        Parameters
        ----------
        job : Job
            Job object.

        Returns
        -------
        None
        """
        raise NotImplementedError

    @abstractmethod
    def batch_jobs(self, jobs: List[Job]) -> None:
        """Places batch of jobs in queue to be batched.

        Parameters
        ----------
        jobs : list(dict)
            List of Jobs.

        Returns
        -------
        None
        """
        raise NotImplementedError

    @abstractmethod
    def _batch(self):
        """Internal method for batching jobs. Implemented methods should
        be destructive (removing jobs from self.jobs) to prevent
        accidental reprocessing of batched jobs."""
        raise NotImplementedError

    def _is_failed_job(self, job: Job) -> bool:
        """Determines whether a job can't be batched based on job size
        and allocated space of workers.

        Parameters
        ----------
        job : dict
            Job object.

        Returns
        -------
        bool
            Whether workers have enough space to process job.
        """
        for worker in self.workers:
            if worker.max_available_space >= job.decompressed_size:
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
        if worker.worker_id in self.worker_dict:
            self.worker_dict[worker.worker_id] = worker
            self._batch()
        else:
            raise ValueError(f"Worker {worker.worker_id} does not already exist")
