import unittest
import uuid
from abyss.orchestrator.worker import Worker
from abyss.batchers.batcher import Batcher


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
    def test_validate_jobs(self):
        workers = []

        for _ in range(10):
            workers.append(Worker(str(uuid.uuid4()),
                                  str(uuid.uuid4()),
                                  0))

        invalid_jobs_1 = [{"filepath": "/",
                           "decompressed_size": 0}]
        invalid_jobs_2 = [{"file_path": "/",
                           "compressed_size": 0}]
        invalid_jobs_3 = [{"file_path": 0,
                           "decompressed_size": 0}]
        invalid_jobs_4 = [{"file_path": "/",
                           "decompressed_size": "0"}]

        with self.assertRaises(ValueError):
            Batcher.validate_jobs(invalid_jobs_1)

        with self.assertRaises(ValueError):
            Batcher.validate_jobs(invalid_jobs_2)

        with self.assertRaises(ValueError):
            Batcher.validate_jobs(invalid_jobs_3)

        with self.assertRaises(ValueError):
            Batcher.validate_jobs(invalid_jobs_4)

    def test_is_failed_job(self):
        workers = []

        for i in range(10):
            workers.append(Worker(str(uuid.uuid4()),
                                  str(uuid.uuid4()),
                                  i))

        jobs = [{"file_path": "/",
                 "decompressed_size": 10},
                {"file_path": "/",
                 "decompressed_size": 5}]

        batcher = DummyBatcher(workers, jobs)

        self.assertEqual([jobs[0]], batcher.failed_jobs)
        self.assertEqual([jobs[1]], batcher.jobs)


if __name__ == '__main__':
    unittest.main()
