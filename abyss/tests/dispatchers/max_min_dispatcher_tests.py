import unittest

from abyss.dispatchers.max_min_dispatcher import MaxMinDispatcher
from abyss.orchestrator.job import Job
from abyss.orchestrator.worker import Worker


class MaxMinFirstDispatcherTests(unittest.TestCase):
    def test_dispatch(self):
        workers = [Worker.from_dict({
            "globus_eid": "0",
            "funcx_eid": "1",
            "max_available_space": 97,
            "transfer_dir": "/transfer",
            "decompress_dir": "/dir",
        })]
        worker_batches = {
            workers[0].worker_id: [Job.from_dict(({"file_path": f"/{i}",
                                                   "compressed_size": 0,
                                                   "decompressed_size": i,
                                                   })) for i in range(1, 11)]
        }

        worker_batch_0 = worker_batches[workers[0].worker_id]
        preserved_batches = {
            workers[0].worker_id: [worker_batch_0[9],
                                   worker_batch_0[0],
                                   worker_batch_0[1],
                                   worker_batch_0[2],
                                   worker_batch_0[3],
                                   worker_batch_0[8],
                                   worker_batch_0[4],
                                   worker_batch_0[5],
                                   worker_batch_0[7],
                                   worker_batch_0[6]]
        }

        dispatcher = MaxMinDispatcher(workers)
        dispatcher.dispatch_batch(worker_batches)
        worker_queues = dispatcher.worker_queues

        for worker_id, worker_queue in worker_queues.items():
            self.assertEqual(list(worker_queue.queue), preserved_batches[worker_id])

        self.assertEqual(worker_batches, {workers[0].worker_id: []})
        self.assertEqual(dispatcher.worker_batches, {workers[0].worker_id: []})


if __name__ == '__main__':
    unittest.main()
