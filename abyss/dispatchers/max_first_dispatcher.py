from typing import List

from abyss.dispatchers.dispatcher import Dispatcher
from abyss.orchestrator.worker import Worker


class MaxFirstDispatcher(Dispatcher):
    def __init__(self, workers: List[Worker]):
        """Dispatches worker_batches into queues max first.

        Parameters
        ----------
        workers : list(Worker)
            List of Worker objects to dispatch jobs for.
        """
        super().__init__(workers)

    def dispatch_batch(self, worker_batches: dict) -> None:
        """Places worker_batches into queues for each worker in max
        first ordering.

        Parameters
        ----------
        worker_batches : dict
            Dictionaries mapping worker ID's to a list of jobs batched
            to the worker via the Batcher class. Worker ID's must
            already be present in worker_batches attribute. After being
            placed into queue, items will be removed from worker_batches.

        Returns
        -------
        None
        """
        for worker_id, worker_batch in worker_batches.items():
            if worker_id not in self.worker_batches:
                raise ValueError(f"Can not insert {worker_id} into worker_batches")

            self.worker_batches[worker_id] = worker_batches[worker_id]
            worker_batches[worker_id] = []

        self._dispatch()

    def _dispatch(self) -> None:
        """Places items from worker batch into worker queue in max first
        ordering.

        Returns
        -------
        None
        """
        for worker_id, worker_batch in self.worker_batches.items():
            worker_queue = self.worker_queues[worker_id]
            worker_batch.sort(reverse=True,
                              key=(lambda x: x.decompressed_size))

            for job in worker_batch:
                worker_queue.append(job)

            self.worker_batches[worker_id] = []
