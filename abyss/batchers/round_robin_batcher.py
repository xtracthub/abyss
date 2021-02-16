from queue import Queue
from typing import Dict, List
from abyss.orchestrator.worker import Worker
from abyss.batchers.batcher import Batcher


class RoundRobinBatcher(Batcher):
    def __init__(self, workers: List[Worker], jobs: List[Dict]):
        super().__init__(workers, jobs)

        self.job_queue = Queue()
        self.num_workers = len(self.workers)
        self.curr_idx = 0

        self.jobs.sort(key=lambda x: x["decompressed_size"])
        for job in self.jobs:
            self.job_queue.put(job)

        self._batch()

    def batch_job(self, job: Dict):
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

    def batch_jobs(self, jobs: List[Dict]):
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
        for job in jobs:
            self.batch_job(job)

    def _batch(self):
        """Schedules jobs using a round robin method. This method goes
        through each item in the queue

        Returns
        -------
        None
        """
        for _ in range(self.job_queue.qsize()):
            job = self.job_queue.get()
            decompressed_size = job["decompressed_size"]

            worker_idx = self.curr_idx
            for idx in range(self.num_workers):
                worker = self.workers[worker_idx]
                if worker.available_space >= decompressed_size:
                    self.worker_batches[worker.worker_id].append(job)
                    worker.available_space -= decompressed_size
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
        print(f"Worker {worker.worker_id}: {worker.available_space} bytes")

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








