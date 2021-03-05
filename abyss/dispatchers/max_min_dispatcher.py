from collections import deque
from typing import List

from abyss.dispatchers.dispatcher import Dispatcher
from abyss.orchestrator.worker import Worker


class MaxMinDispatcher(Dispatcher):
    def __init__(self, workers: List[Worker]):
        """Dispatches worker_batches into queues in max min order.
        First, the largest job is queued. Then, the the smallest jobs
        are queued until the combined size of the small jobs are at
        least the size of the large job. The dispatcher the continues
        this pattern.

        Examples:
        Decompressed sizes of jobs to be queued: [1,2,3,4,5,8,10].

        First, the largest job will be queued.
        queue = [10]

        Then, smallest jobs are queued until combined size == 10.
        queue = [10,1,2,3,4]

        Then, the largest job will be queued.
        queue = [10,1,2,3,4,8]

        Then, smallest jobs are queued until combined size == 8.
        queue = [10,1,2,3,4,8,5]

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
            worker_batch.sort(key=(lambda x: x.decompressed_size))

            worker_deque = deque(worker_batch)

            while worker_deque:
                max_job = worker_deque.pop()
                worker_queue.put(max_job)

                small_job_sizes = 0
                while worker_deque and small_job_sizes < max_job.decompressed_size:
                    min_job = worker_deque.popleft()
                    worker_queue.put(min_job)

                    small_job_sizes += min_job.decompressed_size

            self.worker_batches[worker_id] = []
