import random
import unittest

from abyss.batchers.knapsack_batcher import KnapsackBatcher
from abyss.orchestrator.job import Job
from abyss.orchestrator.worker import Worker


class KnapsackBatcherTests(unittest.TestCase):
    def test_batch(self):
        for _ in range(10):
            workers = [Worker.from_dict({
                "globus_eid": "0",
                "funcx_eid": "1",
                "max_available_space": random.randint(10**11, 10**13),
                "transfer_dir": "/transfer",
                "decompress_dir": "/dir",
            }) for _ in range(random.randint(1, 100))]

            jobs = [Job.from_dict(({
                "file_path": f"/{i}",
                "compressed_size": 0,
                "decompressed_size": random.randint(10**6, 10**10),
            })) for i in range(random.randint(1, 10**3))]

            batcher = KnapsackBatcher(workers, jobs)
            worker_batches = batcher.worker_batches

            for worker_id, worker_batch in worker_batches.items():
                for job in worker_batch:
                    self.assertTrue(job not in batcher.jobs)

if __name__ == '__main__':
    unittest.main()
