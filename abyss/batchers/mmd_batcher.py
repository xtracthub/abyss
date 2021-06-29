import math
from collections import deque
from typing import List
import logging

from abyss.batchers.batcher import Batcher
from abyss.orchestrator.job import Job
from abyss.orchestrator.worker import Worker
from abyss.definitions import ROOT_DIR


logger = logging.getLogger(__name__)
f_handler = logging.FileHandler(f"{ROOT_DIR}/file.log")
f_handler.setLevel(logging.ERROR)
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)


class MMDBatcher(Batcher):
    def __init__(self, workers: List[Worker], jobs: List[Job]):
        """Batches jobs by using a greedy algorithm to minimize the mean
        difference between workers's jobs to create "fair" batching. For
        each job, the batcher chooses to place the job in a worker such
        that the maximum difference between mean job batch size is
        minimized.

        Parameters
        ----------
        workers : list(Worker)
            List of Worker objects to batch jobs amongst.
        jobs : list(Job)
            List of Jobs to batch amongst workers.
        """
        super().__init__(workers, jobs)

        self.job_queue = deque()
        self.num_workers = len(self.workers)
        self.curr_idx = 0

        for job in self.jobs:
            self.job_queue.append(job)

        self.jobs = []

        self._batch()

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
        if self._is_failed_job(job):
            self.failed_jobs.add(job)
        else:
            self.job_queue.append(job)

        self._batch()

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
        for j in jobs:
            logger.error(f"{j.file_path} has been submitted to batcher")

        for job in jobs:
            if self._is_failed_job(job):
                logger.error(f"{job.file_path} failed to be batched")
                self.failed_jobs.add(job)
            else:
                logger.error(f"{job.file_path} waiting to be batched")
                self.job_queue.append(job)

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
            job_batch_size = sum([job.total_size for job in worker_batch])

            worker_info.append([worker.worker_id,
                                job_batch_size,
                                available_space])

        for _ in range(len(self.job_queue)):
            job = self.job_queue.popleft()
            total_size = job.total_size
            worker_info.sort(key=lambda x: (x[1] + total_size)/x[2] if x[2] > 0 else math.inf)

            for idx, worker_info_tuple in enumerate(worker_info):
                worker_id = worker_info_tuple[0]
                worker_batch_size = worker_info_tuple[1]
                available_space = worker_info_tuple[2]

                if worker_batch_size + total_size <= available_space:
                    job.worker_id = worker_id

                    worker = self.worker_dict[worker_id]
                    self.worker_batches[worker_id].append(job)

                    print(f"worker space before batching: {worker.curr_available_space}")
                    worker.curr_available_space -= total_size
                    print(f"worker space after batching: {worker.curr_available_space}")
                    print(f"da job: {Job.to_dict(job)}")
                    worker_info_tuple[1] += total_size
                    break
                elif idx == len(self.workers) - 1:
                    self.job_queue.append(job)
