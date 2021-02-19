import threading
import uuid
from queue import Queue
from typing import Dict, List
from abyss.orchestrator.worker import Worker
from abyss.prefetcher.globus_prefetcher import GlobusPrefetcher
from abyss.batchers import get_batcher
from abyss.predictors import FILE_PREDICTOR_MAPPING
from abyss.predictors.predictor import Predictor
from abyss.dispatcher import get_dispatcher
import time


REQUIRED_ORCHESTRATOR_PARAMETERS = [
            ("globus_source_eid", str),
            ("auth_token", str),
            ("transfer_token", str),
            ("compressed_files", list),
            ("grouper", str),
            ("worker_params", list)
        ]


class AbyssOrchestrator:
    def __init__(self, globus_source_eid: str, auth_token: str,
                 transfer_token: str, compressed_files: List[Dict],
                 worker_params: List[Dict], grouper="", batcher="mmd",
                 dispatcher="fifo"):

        self.globus_source_eid = globus_source_eid
        self.auth_token = auth_token
        self.transfer_token = transfer_token
        self.compressed_files = Queue()
        self.grouper = grouper
        self.workers = []
        self.worker_dict = dict()
        self.predicted_files = []
        self.worker_batches = dict()
        self.predictors = dict()
        self.prefetchers = dict()
        self.job_statuses = dict()
        self.prefetcher_poller = threading.Thread(target=self._thread_poll_prefetch)

        for compressed_file in compressed_files:
            self.compressed_files.put(compressed_file)

            self.job_statuses[compressed_file["file_path"]] = "UNPREDICTED"

        for worker_param in worker_params:
            self.workers.append(Worker.from_dict(worker_param))
        for worker in self.workers:
            self.worker_dict[worker.worker_id] = worker

        for file_type, predictor in FILE_PREDICTOR_MAPPING.items():
            file_predictor = predictor()
            file_predictor.load_model()
            self.predictors[file_type] = file_predictor

        for worker in self.workers:
            globus_dest_eid = worker.globus_eid
            transfer_dir = worker.transfer_dir
            prefetcher = GlobusPrefetcher(self.transfer_token,
                                          self.globus_source_eid,
                                          globus_dest_eid,
                                          transfer_dir,
                                          2)

            self.prefetchers[worker.worker_id] = prefetcher

        self.batcher = get_batcher(batcher)(self.workers)

        # self.prefetcher_poller.start()

        print("WORKERS:")
        for worker in self.workers:
            print(f"{worker.worker_id}: {worker.available_space}")

        print("JOBS:")
        for job in compressed_files:
            print(job)

        print("JOB STATUSES:")
        print(self.job_statuses)

    @staticmethod
    def validate_dict_params(orchestrator_params: Dict) -> None:
        """Ensures dictionary of orchestrator parameters contains
        necessary parameters.

        Parameters
        ----------
        orchestrator_params : dict
            Dictionary containing parameters for AbyssOrchestrator
            object.

        Returns
        -------
            Returns None if parameters are valid, raises error if
            invalid.
        """
        try:
            for parameter_name, parameter_type in REQUIRED_ORCHESTRATOR_PARAMETERS:
                parameter = orchestrator_params[parameter_name]
                assert isinstance(parameter, parameter_type)
        except AssertionError:
            raise ValueError(f"Parameter {parameter_name} is not of type {parameter_type}")
        except KeyError:
            raise ValueError(f"Required parameter {parameter_name} not found")

        worker_params = orchestrator_params["worker_params"]
        for worker_param in worker_params:
            Worker.validate_dict_params(worker_param)

    def start(self):
        threading.Thread(target=self._orchestrate()).start()

    def _orchestrate(self):
        """
        Step 1: Predict sizes of jobs using ML predictors
        Step 2: Batch jobs to worker using Batchers
        Step 3: Begin transferring files one at a time to each worker using
        one Prefetcher item per worker.
        Step 4: Constantly poll prefetcher for file completion.
        Step 5: When a file is done, send a funcx job request to crawl on worker
        Step 6: Poll funcx result
        Step 7: Pull result from sqs queue and validate/consolidate

        Returns
        -------

        """
        self._predict_decompressed_size()
        print(f'PREDICTED SIZES:')
        print(f"{self.predicted_files}")
        print("JOB STATUSES:")
        print(f"{self.job_statuses}")
        print("hello")
        self._schedule_jobs()
        print("WORKER BATCHES:")
        print(f"{self.worker_batches}")
        for worker_id, job_batch in self.worker_batches.items():
            print(f"WORKER ID: {worker_id}")
            print(f"BATCH SIZE: {sum([x['decompressed_size'] for x in job_batch])}")
        print("JOB STATUSES:")
        print(f"{self.job_statuses}")
        # while not(all([status in ["FAILED", "SUCCESS"] for status in self.job_statuses.keys()])):
        #     self._predict_decompressed_size()
        #     self._schedule_jobs()
        #     time.sleep(10)

    def _predict_decompressed_size(self):
        """Runs decompression size predictions on all files in
        self.compressed_files and then places them in
        self.predicted_files.

        Returns
        -------
        None
        """
        while not(self.compressed_files.empty()):
            compressed_file = self.compressed_files.get()

            file_path = compressed_file["file_path"]
            compressed_size = compressed_file["compressed_size"]
            file_extension = Predictor.get_extension(file_path)

            predictor = self.predictors[file_extension]
            decompressed_size = predictor.predict(file_path, compressed_size)

            self.predicted_files.append({"file_path": file_path,
                                         "decompressed_size": decompressed_size})
            self.job_statuses[file_path] = "PREDICTED"

    def _schedule_jobs(self):
        """Batches and dispatches items from self.predicted_files into
        worker batches in self.worker_batches.

        Returns
        -------
        None
        """
        self.batcher.batch_jobs(self.predicted_files)
        self.worker_batches = self.batcher.worker_batches
        self.predicted_files = []

        for job_batch in self.worker_batches.values():
            for job in job_batch:
                file_path = job["file_path"]
                self.job_statuses[file_path] = "SCHEDULED"

    def _prefetch(self):
        """Places jobs into queue for prefetcher to transfer.

        Returns
        -------
        None
        """
        for worker_id, job_batch in self.worker_batches.items():
            prefetcher = self.prefetchers[worker_id]

            for job in job_batch:
                file_path = job["file_path"]

                if self.job_statuses[file_path] == "SCHEDULED":
                    prefetcher.transfer(file_path)

                    self.job_statuses[file_path] = f"PREFETCHER: {prefetcher.get_transfer_status(file_path)}"

    def _thread_poll_prefetch(self):
        """Thread function to poll prefetcher and update
        self.job_statuses.

        Returns
        -------
        None
        """
        while not(all([status in ["FAILED", "SUCCESS"] for status in self.job_statuses.keys()])):
            for worker_id, job_batch in self.worker_batches.items():
                for job in job_batch:
                    file_path = job["file_path"]
                    job_status = self.job_statuses[file_path]

                    if job_status in ["PREFETCHER: QUEUED", "PREFETCHER: ACTIVE"]:
                        prefetcher = self.prefetchers[worker_id]

                        self.job_statuses[file_path] = f"PREFETCHER: {prefetcher.get_transfer_status(file_path)}"
                    elif job_status == "PREFETCHER: SUCCESS":
                        self.job_statuses[file_path] = "SUCCESS"
                    elif job_status == "PREFETCHER: FAILED":
                        self.job_statuses[file_path] = "FAILED"


if __name__ == "__main__":
    import pandas as pd
    import os
    PROJECT_ROOT = os.path.realpath(os.path.dirname(__file__)) + "/"
    print(PROJECT_ROOT)
    deep_blue_crawl_df = pd.read_csv("/Users/ryan/Documents/CS/abyss/data/deep_blue_crawl.csv")
    filtered_files = deep_blue_crawl_df[deep_blue_crawl_df.extension == "gz"].sort_values(by=["size_bytes"]).iloc[1:20]

    workers = [Worker("49f1efac-6049-11eb-87c8-02187389bd35",
                      str(uuid.uuid4()),
                      "/home/tskluzac/ryan/deep_blue_data",
                      "/home/tskluzac/ryan/results",
                      10**9)]
    worker_params = [{
        "globus_eid": "49f1efac-6049-11eb-87c8-02187389bd35",
        "funcx_eid": str(uuid.uuid4()),
        "transfer_dir": "/home/tskluzac/ryan/deep_blue_data",
        "decompress_dir": "/home/tskluzac/ryan/results",
        "available_space": 10**9
    }]

    compressed_files = [{"file_path": x[0], "compressed_size": x[1]} for _, x in filtered_files.iterrows()]
    auth_token = 'Bearer Age3y9kq07VaV5wJ2EoyG5QKYGJdrQyqOnpbylB251bNy0PEr8IVC1n6zMYJ88ByvPqJqe5x865pbzfK2bd4pFlVo4'
    transfer_token = 'AgVXGVEYKx2WN64NQqxJqq6Xbj7neX2r48BMMplEaekqyJ46OgSbCdz3a9rzwOB8bNNzgKwW1mOvKgSlVzgwWspD3o'

    orchestrator = AbyssOrchestrator("4f99675c-ac1f-11ea-bee8-0e716405a293",
                                     auth_token,transfer_token,compressed_files,
                                     worker_params,batcher="knapsack")

    orchestrator._orchestrate()


