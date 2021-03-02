import math
from queue import Queue
from typing import Dict, List

from abyss.batchers.batcher import Batcher
from abyss.orchestrator.worker import Worker


class MMDBatcher(Batcher):
    def __init__(self, workers: List[Worker], jobs: List[Dict]):
        """Batches jobs by using a greedy algorithm to minimize the mean
        difference between workers's jobs to create "fair" batching. For
        each job, the batcher chooses to place the job in a worker such
        that the maximum difference between mean job batch size is
        minimized.

        Parameters
        ----------
        workers : list(Worker)
            List of Worker objects to batch jobs amongst.
        jobs : list(dict)
            List of jobs (dictionaries containing file_path and
            decompressed_size) to batch amongst workers.
        """
        super().__init__(workers, jobs)

        self.job_queue = Queue()
        self.num_workers = len(self.workers)
        self.curr_idx = 0

        for job in self.jobs:
            self.job_queue.put(job)

        self._batch()

    def batch_job(self, job: Dict) -> None:
        """Places job in queue to be scheduled.

        Parameters
        ----------
        job : dict
            Dictionary with file path and size of decompressed file.

        Returns
        -------
        None
        """
        self.validate_jobs([job])

        if self._is_failed_job(job):
            self.failed_jobs.append(job)
        else:
            self.job_queue.put(job)

        self._batch()

    def batch_jobs(self, jobs: List[Dict]) -> None:
        """Places batch of jobs in queue to be scheduled.

        Parameters
        ----------
        jobs : list(dict)
            List of dictionaries with file path and size of decompressed
            file.

        Returns
        -------
        None
        """
        self.validate_jobs(jobs)

        for job in jobs:
            if self._is_failed_job(job):
                self.failed_jobs.append(job)
            else:
                self.job_queue.put(job)

        self._batch()

    def _batch(self) -> None:
        """Iterates through each job and adds job to worker that will
        have the lowest mean job batch size once the job is added.

        Returns
        -------
        None
        """
        worker_info = []
        for worker in self.workers:
            worker_batch = self.worker_batches[worker.worker_id]
            available_space = worker.curr_available_space
            job_batch_size = sum([job["decompressed_size"] for job in worker_batch])

            worker_info.append([worker.worker_id,
                                job_batch_size,
                                available_space])

        for _ in range(self.job_queue.qsize()):
            job = self.job_queue.get()
            decompressed_size = job["decompressed_size"]
            worker_info.sort(key=lambda x: (x[1] + decompressed_size)/x[2] if x[2] > 0 else math.inf)

            for idx, worker_info_tuple in enumerate(worker_info):
                worker_id = worker_info_tuple[0]
                worker_batch_size = worker_info_tuple[1]
                available_space = worker_info_tuple[2]

                if worker_batch_size + decompressed_size <= available_space:
                    worker = self.worker_dict[worker_id]
                    self.worker_batches[worker_id].append(job)

                    worker.available_space -= decompressed_size
                    worker_info_tuple[1] += decompressed_size
                    break
                elif idx == len(self.workers) - 1:
                    self.job_queue.put(job)






