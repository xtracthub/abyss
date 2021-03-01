from abc import ABC, abstractmethod
from queue import Queue
from typing import Dict, List
from abyss.orchestrator.worker import Worker


class Dispatcher(ABC):
    def __init__(self, workers: List[Worker], worker_batches: Dict[str, List]):
        """Base class for creating dispatchers. Dispatchers take in
        worker objects and batches of jobs for each worker and creates
        worker queues that order jobs for workers to complete.

        Parameters
        ----------
        workers : list(Worker)
            List of Worker objects to batch jobs amongst.
        worker_batches : dict(str, list)
            Dictionaries mapping worker ID's to a list of jobs batched
            to the worker via the Batcher class.
        """
        self.workers = workers
        self.worker_batches = worker_batches
        self.worker_queues = dict()

        for worker in workers:
            self.worker_queues[worker.worker_id] = []

    @abstractmethod
    def dispatch_batch(self, worker_batches: Dict):
        raise NotImplementedError

    @abstractmethod
    def _dispatch(self):
        raise NotImplementedError
