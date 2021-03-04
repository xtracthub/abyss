import unittest

from abyss.orchestrator.worker import Worker


class WorkerTests(unittest.TestCase):
    def test_validate_dict_params(self):
        with self.assertRaises(ValueError):
            bad_dict_params = {
                "globus_eid": 10,
                "funcx_eid": "1",
                "max_available_space": 10,
                "transfer_dir": "/transfer",
                "decompress_dir": "/dir"
            }
            Worker.validate_dict_params(bad_dict_params)

        with self.assertRaises(ValueError):
            bad_dict_params_1 = {
                "globus_eid": "0",
                "funcx_eid": "1",
                "max_available_space": 10,
                "transfer_dir": "/transfer"
            }
            Worker.validate_dict_params(bad_dict_params_1)

        with self.assertRaises(ValueError):
            bad_dict_params_2 = {
                "globus_eid": "0",
                "funcx_eid": "1",
                "max_available_space": 10,
                "transfer_dir": "/transfer",
                "decompress_dir": "/dir",
                "this should": "have an effect"
            }
            Worker.validate_dict_params(bad_dict_params_2)

        good_dict_params = {
            "globus_eid": "0",
            "funcx_eid": "1",
            "max_available_space": 10,
            "transfer_dir": "/transfer",
            "decompress_dir": "/dir"
        }
        Worker.validate_dict_params(good_dict_params)

    def test_from_dict(self):
        good_dict_params = {
            "globus_eid": "0",
            "funcx_eid": "1",
            "max_available_space": 10,
            "transfer_dir": "/transfer",
            "decompress_dir": "/dir",
        }

        worker = Worker.from_dict(good_dict_params)

        self.assertEqual(worker.globus_eid,
                         good_dict_params["globus_eid"])
        self.assertEqual(worker.funcx_eid,
                         good_dict_params["funcx_eid"])
        self.assertEqual(worker.max_available_space,
                         good_dict_params["max_available_space"])
        self.assertEqual(worker.transfer_dir,
                         good_dict_params["transfer_dir"])
        self.assertEqual(worker.decompress_dir,
                         good_dict_params["decompress_dir"])


if __name__ == '__main__':
    unittest.main()
