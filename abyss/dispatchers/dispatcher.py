from abc import ABC, abstractmethod
from collections import deque
from typing import List

from abyss.orchestrator.worker import Worker


class Dispatcher(ABC):
    def __init__(self, workers: List[Worker]):
        """Base class for creating dispatchers. Dispatchers take in
        worker objects and batches of jobs for each worker and creates
        worker queues that order jobs for workers to complete.

        Parameters
        ----------
        workers : list(Worker)
            List of Worker objects to batch jobs amongst.
        """
        self.workers = workers
        self.worker_batches = dict()
        self.worker_queues = dict()

        for worker in workers:
            self.worker_queues[worker.worker_id] = deque()
            self.worker_batches[worker.worker_id] = []

    @abstractmethod
    def dispatch_batch(self, worker_batches: dict) -> None:
        """Places worker_batches into queues for each worker.

        Parameters
        ----------
        worker_batches : dict
            Dictionaries mapping worker ID's to a list of jobs batched
            to the worker via the Batcher class. Worker ID's must
            already be present in worker_batches attribute.

        Returns
        -------
        None
        """
        raise NotImplementedError

    @abstractmethod
    def _dispatch(self) -> None:
        """Internal method for placing batches into queues."""
        raise NotImplementedError
