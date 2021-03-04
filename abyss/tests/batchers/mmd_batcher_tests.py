import unittest

from abyss.batchers.mmd_batcher import MMDBatcher
from abyss.orchestrator.job import Job
from abyss.orchestrator.worker import Worker


class MMDBatcherTests(unittest.TestCase):
    def test_batch(self):
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
        jobs = [Job.from_dict(({
            "file_path": f"/{i}",
            "compressed_size": 0,
            "decompressed_size": i,
        })) for i in range(10, 20)]

        batcher = MMDBatcher(workers, jobs)
        batches = batcher.worker_batches

        batch_0 = batches[workers[0].worker_id]
        self.assertEqual(set([job.decompressed_size for job in batch_0]),
                         {10, 12, 13, 15, 16, 18})

        batch_1 = batches[workers[1].worker_id]
        self.assertEqual(set([job.decompressed_size for job in batch_1]),
                         {11, 14, 17})

        queued_jobs = []
        while not batcher.job_queue.empty():
            queued_jobs.append(batcher.job_queue.get())

        self.assertEqual(set([job.decompressed_size for job in queued_jobs]),
                         {19})

        for _, worker_batch in batches.items():
            for job in worker_batch:
                self.assertTrue(job not in batcher.jobs)

    def test_multiple_batch(self):
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
        jobs = [Job.from_dict(({
            "file_path": f"/{i}",
            "compressed_size": 0,
            "decompressed_size": i,
        })) for i in range(10, 20)]

        batcher = MMDBatcher(workers, jobs)
        batches = batcher.worker_batches

        queued_jobs = []
        for _ in range(batcher.job_queue.qsize()):
            job = batcher.job_queue.get()
            queued_jobs.append(job)
            batcher.job_queue.put(job)

        self.assertEqual(set([job.decompressed_size for job in queued_jobs]),
                         {19})

        batcher.batch_job(Job.from_dict(({
            "file_path": f"/{100}",
            "compressed_size": 0,
            "decompressed_size": 100,
        })))
        batches_1 = batcher.worker_batches

        self.assertEqual(batches, batches_1)


if __name__ == '__main__':
    unittest.main()
