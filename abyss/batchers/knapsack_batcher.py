import numpy as np
import math
from typing import Dict, List
from abyss.batchers.batcher import Batcher
from abyss.orchestrator.worker import Worker


class KnapsackBatcher(Batcher):
    def __init__(self, workers: List[Worker], **kwargs):
        """Batches using a Knapsack heuristic. The purpose of
        this batcher is to maximize the size of each worker batch while
        ensuring that batch size < worker available space. This is
        analogous to the 0-1 Knapsack problem, where worker available
        space is the capacity of the knapsack, and a job's size is the
        weight and value of items to place in the knapsack.

        Using dynamic programming, the knapsack algorithm has a time
        complexity O(N*C) and space complexity O(N*C), where N is the
        number of items and C is the capacity of the knapsack. Since C
        is the amount of space on a worker, C is in the Giga-Terabyte
        range (10^9 - 10^11), which is infeasible for space and time
        constraints. To solve this, a capacity_buffer variable is used
        to divide the size of jobs and available space in order to
        reduce the size and time complexity of this algorithm. With a
        capacity buffer B, the time and space complexity is now
        O(N*C/B). However, this results in wasted space with an upper
        bound of N*B.

        Since this batcher calculates the optimal batch for each worker
        independently from each other, this heuristic produces a locally
        correct solution for each batch but not a globally correct
        solution for all workers. Additionally, this heuristic may
        result in unbalanced batches, as each batch is filled to
        its max one at a time.

        Parameters
        ----------
        workers : list(Worker)
            List of Worker objects to batch jobs amongst.
        jobs : list(dict)
            List of jobs (dictionaries containing file_path and
            decompressed_size) to batch amongst workers.
        kwargs
            capacity_buffer : int
                Factor to divide available space and job size by.
                Defaults to 10 ** 7 (1MB).
        """
        super().__init__(workers)

        if "capacity_buffer" in kwargs:
            self.capacity_buffer = kwargs["capacity_buffer"]
        else:
            self.capacity_buffer = 10 ** 7

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
            self.jobs.append(job)

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
                self.jobs.append(job)

        self._batch()

    def _batch(self) -> None:
        """Schedules using a Knapsack heuristic. Goes through each
        worker and determines the combination of jobs which best
        maximizes the job batch size using the 0-1 Knapsack algorithm.

        Returns
        -------
        None
        """
        for worker in self.workers:
            jobs = self._get_knapsack_items(worker)

            assert sum([job["decompressed_size"] for job in jobs]) <= worker.available_space

            for job in jobs:
                decompressed_size = job["decompressed_size"]
                worker.available_space -= decompressed_size

                self.worker_batches[worker.worker_id].append(job)
                self.jobs.remove(job)

    def _get_knapsack_items(self, worker) -> List[Dict]:
        """Determines job batch for single worker using 0-1 Knapsack
        algorithm.

        Parameters
        ----------
        worker : Worker
            Worker to batch jobs for.

        Returns
        -------
        jobs : list(dict)
            Job batch for worker.
        """
        available_space = math.ceil(worker.available_space / self.capacity_buffer)
        knapsack_array = np.empty((len(self.jobs) + 1,
                                   available_space + 1))

        for i in range(len(self.jobs) + 1):
            last_item_size = math.ceil(self.jobs[i - 1]["decompressed_size"] / self.capacity_buffer)
            for j in range(available_space + 1):
                if i == 0 or j == 0:
                    knapsack_array[i][j] = 0
                elif last_item_size <= j:

                    knapsack_array[i][j] = max(last_item_size + knapsack_array[i - 1][j - last_item_size],
                                               knapsack_array[i - 1][j])
                else:
                    knapsack_array[i][j] = knapsack_array[i - 1][j]

        res = knapsack_array[len(self.jobs)][available_space]
        jobs = []

        for i in range(len(self.jobs), 0, -1):
            if res <= 0:
                break

            if res == knapsack_array[i - 1][available_space]:
                continue
            else:
                jobs.append(self.jobs[i - 1])
                res -= math.ceil(self.jobs[i - 1]["decompressed_size"] / self.capacity_buffer)
                available_space -= math.ceil(self.jobs[i - 1]["decompressed_size"] / self.capacity_buffer)

        return jobs

