import unittest

from abyss.batchers import BATCHER_NAME_MAPPING
from abyss.dispatchers import DISPATCHER_NAME_MAPPING
from abyss.schedulers.scheduler import Scheduler
from abyss.orchestrator.job import Job
from abyss.orchestrator.worker import Worker


class SchedulerTests(unittest.TestCase):
    def test_batch(self):
        for batcher_name in BATCHER_NAME_MAPPING.keys():
            for dispatcher_name in DISPATCHER_NAME_MAPPING.keys():
                workers = [Worker.from_dict({
                    "globus_eid": "0",
                    "funcx_eid": "1",
                    "max_available_space": 10,
                    "transfer_dir": "/transfer",
                    "decompress_dir": "/dir",
                })]
                jobs = [Job.from_dict(({
                                           "file_path": f"/0",
                                           "compressed_size": 0,
                                           "decompressed_size": 10,
                                       })),
                    Job.from_dict(({
                        "file_path": f"/1",
                        "compressed_size": 0,
                        "decompressed_size": 20,
                    }))]

                scheduler = Scheduler(batcher_name, dispatcher_name,
                                      workers, jobs)
                worker_queues = scheduler.worker_queues

                # Making sure only the correct job gets scheduled
                self.assertTrue(len(worker_queues), 1)

                worker_queue_0 = worker_queues[workers[0].worker_id]
                self.assertEqual(len(worker_queue_0), 1)
                self.assertEqual(worker_queue_0[0].decompressed_size,
                                 10)

                self.assertTrue(worker_queue_0[0] not in scheduler._batcher.jobs)

                # Making sure no jobs get batched twice
                scheduler.schedule_jobs([])

                new_worker_queues = scheduler.worker_queues

                self.assertEqual(len(new_worker_queues), 1)

                new_worker_queue_0 = new_worker_queues[workers[0].worker_id]
                self.assertEqual(len(new_worker_queue_0), 1)
                self.assertEqual(new_worker_queue_0[0].decompressed_size,
                                 10)

                # Making sure jobs are appropriately batched after freeing space
                workers[0].curr_available_space += 50

                scheduler.schedule_jobs([])

                new_worker_queues_1 = scheduler.worker_queues

                self.assertEqual(len(new_worker_queues_1), 1)

                new_worker_queue_1 = new_worker_queues[workers[0].worker_id]
                self.assertEqual(len(new_worker_queue_1), 2)
                self.assertEqual(new_worker_queue_1[0].decompressed_size,
                                 10)
                self.assertEqual(new_worker_queue_1[1].decompressed_size,
                                 20)


if __name__ == '__main__':
    unittest.main()
