import unittest

from abyss.batchers.batcher import Batcher
from abyss.orchestrator.job import Job
from abyss.orchestrator.worker import Worker


class DummyBatcher(Batcher):
    def __init__(self, workers, jobs):
        super().__init__(workers, jobs)

    def batch_job(self, job):
        pass

    def batch_jobs(self, jobs):
        pass

    def _batch(self):
        pass


class BatcherTests(unittest.TestCase):
    def test_is_failed_job(self):
        workers = []

        for i in range(10):
            workers.append(Worker(None, None, None, None,
                                  i))

        jobs = [Job.from_dict({"file_path": "/",
                               "compressed_size": 0,
                               "decompressed_size": 10}),
                Job.from_dict({"file_path": "/",
                               "compressed_size": 0,
                               "decompressed_size": 5})]

        batcher = DummyBatcher(workers, jobs)

        self.assertTrue(batcher._is_failed_job(jobs[0]))
        self.assertFalse(batcher._is_failed_job(jobs[1]))

    def test_update_worker(self):
        workers = [Worker.from_dict({
            "globus_eid": "0",
            "funcx_eid": "1",
            "max_available_space": 97,
            "transfer_dir": "/transfer",
            "decompress_dir": "/dir",
        }),
            Worker.from_dict({
                "globus_eid": "0",
                "funcx_eid": "1",
                "max_available_space": 57,
                "transfer_dir": "/transfer",
                "decompress_dir": "/dir",
            })
        ]
        jobs = [Job.from_dict({"file_path": "/",
                               "compressed_size": 0,
                               "decompressed_size": 10}),
                Job.from_dict({"file_path": "/",
                               "compressed_size": 0,
                               "decompressed_size": 5})]

        worker_0 = workers[0]

        batcher = DummyBatcher(workers, jobs)
        worker_0.curr_available_space += 100

        batcher.update_worker(worker_0)
        self.assertEqual(worker_0.curr_available_space,
                         batcher.worker_dict[worker_0.worker_id].curr_available_space)

        with self.assertRaises(ValueError):
            batcher.update_worker(Worker.from_dict({
                "globus_eid": "0",
                "funcx_eid": "1",
                "max_available_space": 57,
                "transfer_dir": "/transfer",
                "decompress_dir": "/dir",
            }))


if __name__ == '__main__':
    unittest.main()
