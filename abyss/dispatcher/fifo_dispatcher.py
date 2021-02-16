from typing import Dict, List
from abyss.orchestrator.worker import Worker
from abyss.dispatcher.dispatcher import Dispatcher


class FIFODispatcher(Dispatcher):
    def __init__(self, workers: List[Worker], worker_batches: Dict[str, List]):
        super().__init__(workers, worker_batches)

    def dispatch_batch(self, worker_batches: Dict):
        """Destructive function that replaces worker batch in
        worker_batches attribute with new worker batch.

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
        for worker_id, worker_batch in worker_batches.items():
            if worker_id not in self.worker_batches:
                raise ValueError(
                    f"Can not insert {worker_id} into worker_batches")

            self.worker_batches[worker_id] = worker_batches

        self._dispatch()

    def _dispatch(self):
        """Places items from worker batch into worker queue based on
        order of worker batch.

        Returns
        -------
        None
        """
        for worker_id, worker_batch in self.worker_batches.itmes():
            worker_queue = self.worker_queues[worker_id]

            for job in worker_batch:
                worker_queue.put(job)

            self.worker_batches[worker_id] = []
