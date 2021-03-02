from queue import Queue
from typing import List

from abyss.batchers.batcher import Batcher
from abyss.orchestrator.job import Job
from abyss.orchestrator.worker import Worker


class RoundRobinBatcher(Batcher):
    def __init__(self, workers: List[Worker], jobs: List[Job]):
        """Batches jobs using a round robin method.

        Parameters
        ----------
        workers : list(Worker)
            List of Worker objects to batch jobs amongst.
        jobs : list(Job)
            List of Jobs to batch amongst workers.
        """
        super().__init__(workers, jobs)

        self.job_queue = Queue()
        self.num_workers = len(self.workers)
        self.curr_idx = 0

        for job in self.jobs:
            self.job_queue.put(job)

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
            self.failed_jobs.append(job)
        else:
            self.job_queue.put(job)

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
        for job in jobs:
            if self._is_failed_job(job):
                self.failed_jobs.append(job)
            else:
                self.job_queue.put(job)

        self._batch()

    def _batch(self) -> None:
        """Schedules jobs using a round robin method. This method goes
        through each item in the job queue and batches jobs to workers
        in a cyclic fashion.

        Returns
        -------
        None
        """
        for _ in range(self.job_queue.qsize()):
            job = self.job_queue.get()
            decompressed_size = job.decompressed_size

            worker_idx = self.curr_idx
            for idx in range(self.num_workers):
                worker = self.workers[worker_idx]
                if worker.curr_available_space >= decompressed_size:
                    self.worker_batches[worker.worker_id].append(job)
                    worker.curr_available_space -= decompressed_size
                    break
                elif idx == self.num_workers - 1:
                    self.job_queue.put(job)

                worker_idx = (worker_idx + 1) % self.num_workers

            self.curr_idx = (self.curr_idx + 1) % self.num_workers


if __name__ == "__main__":
    import uuid
    import random
    workers = []
    jobs = []

    for _ in range(3):
        workers.append(Worker(str(uuid.uuid4()),
                              str(uuid.uuid4()),
                              random.randint(1, 10)))

    for worker in workers:
        print(f"Worker {worker.worker_id}: {worker.curr_available_space} bytes")

    for i in range(10):
        jobs.append({"file_path": f"{i}",
                     "decompressed_size": random.randint(1, 5)})

    batcher = RoundRobinBatcher(workers, jobs)
    print(f"Failed jobs {batcher.failed_jobs}")

    for worker_id, batch in batcher.worker_batches.items():
        print(f"Worker {worker_id}:")
        print(batch)

    queued = []
    while not(batcher.job_queue.empty()):
        queued.append(batcher.job_queue.get())
    print(f"Jobs in queue: {queued}")








