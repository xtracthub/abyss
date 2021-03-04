import unittest

from abyss.orchestrator.job import Job, JobStatus


class JobTests(unittest.TestCase):
    def test_validate_dict_params(self):
        with self.assertRaises(ValueError):
            bad_dict_params = {
                "file_path": 10,
                "compressed_size": 10
            }
            Job.validate_dict_params(bad_dict_params)

        with self.assertRaises(ValueError):
            bad_dict_params_1 = {
                "compressed_size": 10
            }
            Job.validate_dict_params(bad_dict_params_1)

        good_dict_params = {
            "file_path": "/",
            "compressed_size": 10
        }
        Job.validate_dict_params(good_dict_params)

    def test_from_dict(self):
        with self.assertRaises(ValueError):
            bad_dict_params_1 = {
                "compressed_size": 10
            }
            Job.from_dict(bad_dict_params_1)

        good_dict_params = {
            "file_path": "/",
            "compressed_size": 0,
            "decompressed_size": 0,
            "worker_id": "1",
            "transfer_path": "/transfer",
            "decompress_path": "/decompress",
            "funcx_decompress_id": "2",
            "funcx_crawl_id": "3",
            "status": JobStatus.UNPREDICTED
        }

        job = Job.from_dict(good_dict_params)
        self.assertEqual(job.file_path, good_dict_params["file_path"])
        self.assertEqual(job.compressed_size,
                         good_dict_params["compressed_size"])
        self.assertEqual(job.decompressed_size,
                         good_dict_params["decompressed_size"])
        self.assertEqual(job.worker_id, good_dict_params["worker_id"])
        self.assertEqual(job.transfer_path,
                         good_dict_params["transfer_path"])
        self.assertEqual(job.decompress_path,
                         good_dict_params["decompress_path"])
        self.assertEqual(job.funcx_decompress_id,
                         good_dict_params["funcx_decompress_id"])
        self.assertEqual(job.funcx_crawl_id,
                         good_dict_params["funcx_crawl_id"])
        self.assertEqual(job.status, good_dict_params["status"])


if __name__ == '__main__':
    unittest.main()
