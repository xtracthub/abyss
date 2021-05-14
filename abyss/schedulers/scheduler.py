from typing import Dict, List
import logging

from abyss.batchers import get_batcher
from abyss.dispatchers import get_dispatcher
from abyss.orchestrator.job import Job
from abyss.orchestrator.worker import Worker

logger = logging.getLogger(__name__)
f_handler = logging.FileHandler('/Users/ryan/Documents/CS/abyss/abyss/orchestrator/file.log')
f_handler.setLevel(logging.ERROR)
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)


class Scheduler:
    def __init__(self, batcher_name: str, dispatcher_name: str,
                 workers: List[Worker], jobs: List[Job]):
        """Class for internally managing both the Batcher and
        Dispatcher. Takes jobs and places them into queues for workers
        to process.

        Parameters
        ----------
        batcher_name : str
            Name of Batcher to use.
        dispatcher_name : str
            Name of Dispatcher to use
        workers : list(Worker)
            List of Worker objects to batch jobs amongst.
        jobs : list(dict)
            List of jobs (dictionaries containing file_path and
            decompressed_size) to batch amongst workers.
        """
        self._worker_dict = dict()
        self._batcher = get_batcher(batcher_name)(workers, jobs)
        self._dispatcher = get_dispatcher(dispatcher_name)(workers)

        self.worker_queues = self._dispatcher.worker_queues
        self.failed_jobs = self._batcher.failed_jobs

        for worker in workers:
            self._worker_dict[worker.worker_id] = worker

        self._schedule()

    def update_worker(self, worker: Worker) -> None:
        """Updates a worker. If the current available size of a worker
        is changed, then new items may be added to worker queues..

        Parameters
        ----------
        worker : Worker
            Worker to be updated.

        Returns
        -------
        None
        """
        self._batcher.update_worker(worker)
        self._schedule()

    def schedule_job(self, job: Job) -> None:
        """Places job in queue to be scheduled.

        Parameters
        ----------
        job : Job
            Job object.

        Returns
        -------
        None
        """
        self._batcher.batch_job(job)
        self._schedule()

    def schedule_jobs(self, jobs: List[Job]) -> None:
        """Places batch of jobs in queue to be batched.

        Parameters
        ----------
        jobs : list(Job)
            List of Jobs.

        Returns
        -------
        None
        """
        for j in jobs:
            logger.error(f"{j.file_path} has been submitted to scheduler")

        self._batcher.batch_jobs(jobs)

        for worker_queue in self._batcher.worker_batches.values():
            for i in range(len(worker_queue)):
                logger.error(f"{worker_queue[i].file_path} has been batched")

        self._schedule()

    def _schedule(self) -> None:
        """Internal method for batching and placing jobs in worker
        queues.

        Returns
        -------
        None
        """
        self._dispatcher.dispatch_batch(self._batcher.worker_batches)
        self._worker_queues = self._dispatcher.worker_queues
        self.failed_jobs = self._batcher.failed_jobs

