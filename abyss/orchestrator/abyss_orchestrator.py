import logging
import threading
import time
import uuid
from queue import Queue
from typing import Dict, List

from funcx import FuncXClient

from abyss.decompressors import DECOMPRESSOR_FUNCX_UUID
from abyss.decompressors import is_compressed
from abyss.crawlers.globus_crawler.globus_crawler import GLOBUS_CRAWLER_FUNCX_UUID
from abyss.orchestrator.job import Job, JobStatus
from abyss.orchestrator.worker import Worker
from abyss.predictors import FILE_PREDICTOR_MAPPING
from abyss.predictors.predictor import Predictor
from abyss.prefetcher.globus_prefetcher import GlobusPrefetcher, \
    PrefetcherStatuses
from abyss.schedulers.scheduler import Scheduler
from abyss.utils.psql_utils import update_table_entry
from abyss.utils.sqs_utils import make_queue, put_message

REQUIRED_ORCHESTRATOR_PARAMETERS = [
            ("globus_source_eid", str),
            ("transfer_token", str),
            ("compressed_files", list),
            ("worker_params", list)
        ]

logger = logging.getLogger(__name__)


class AbyssOrchestrator:
    def __init__(self, abyss_id: str, globus_source_eid: str, transfer_token: str,
                 compressed_files: List[Dict], worker_params: List[Dict],
                 psql_conn, sqs_conn, grouper="", batcher="mmd",
                 dispatcher="fifo"):
        """Abyss orchestrator class.
        Parameters
        ----------
        abyss_id : str
            Abyss ID for orchestration.
        globus_source_eid : str
            Globus endpoint of source data storage.
        transfer_token : str
            Globus token to authorize transfers between endpoints.
        compressed_files : list(dict)
            List of dictionaries for compressed files to process.
            Dictionaries contain "file_path" and "compressed_size".
        worker_params : list(dict)
            List of valid worker parameter dictionaries to create
            workers.
        psql_conn :
            PostgreSQL connection object to update status.
        sqs_conn :
            SQS connection object to push results to SQS.
        grouper : str
            Name of grouper to use when crawling.
        batcher : str
            Name of batcher to use.
        dispatcher : str
            Name of dispatchers to use.
        """
        self.abyss_id = abyss_id
        self.globus_source_eid = globus_source_eid
        self.transfer_token = transfer_token
        self.grouper = grouper

        self.worker_dict = dict()
        for worker_param in worker_params:
            worker = Worker.from_dict(worker_param)
            self.worker_dict[worker.worker_id] = worker

        self.prefetchers = dict()
        for worker in self.worker_dict.values():
            globus_dest_eid = worker.globus_eid
            transfer_dir = worker.transfer_dir
            prefetcher = GlobusPrefetcher(self.transfer_token,
                                          self.globus_source_eid,
                                          globus_dest_eid,
                                          transfer_dir,
                                          4)

            self.prefetchers[worker.worker_id] = prefetcher

        self.predictors = dict()
        for file_type, predictor in FILE_PREDICTOR_MAPPING.items():
            file_predictor = predictor()
            file_predictor.load_model()
            self.predictors[file_type] = file_predictor

        self.job_statuses = dict(zip([x for x in JobStatus],
                                     [set() for _ in range(len(JobStatus))]))
        for compressed_file in compressed_files:
            unpredicted_set = self.job_statuses[JobStatus.UNPREDICTED]
            unpredicted_set.add(Job.from_dict(compressed_file))

        self.scheduler = Scheduler(batcher, dispatcher,
                                   list(self.worker_dict.values()), [])

        self.psql_conn = psql_conn
        self.sqs_conn = sqs_conn
        self.sqs_queue_name = f"abyss_{self.abyss_id}"
        make_queue(self.sqs_conn, self.sqs_queue_name)

        self._predictor_thread = threading.Thread(target=self._predict_decompressed_size,
                                                  daemon=True)
        self._scheduler_thread = threading.Thread(target=self._thread_schedule_jobs,
            daemon=True)
        self._prefetcher_thread = threading.Thread(
            target=self._thread_prefetch,
            daemon=True)
        self._prefetcher_poll_thread = threading.Thread(
            target=self._thread_poll_prefetch,
            daemon=True)
        self._funcx_decompress_thread = threading.Thread(
            target=self._thread_funcx_decompress,
            daemon=True)
        self._funcx_crawl_thread = threading.Thread(
            target=self._thread_funcx_crawl,
            daemon=True)
        self._funcx_poll_thread = threading.Thread(
            target=self._thread_funcx_poll,
            daemon=True)
        self._consolidate_results_thread = threading.Thread(
            target=self._thread_consolidate_crawl_results,
            daemon=True)
        self._lock = threading.Lock()

        self.funcx_client = FuncXClient()
        self.kill_status = False
        self.crawl_results = Queue()

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

    def start(self) -> None:
        threading.Thread(target=self._orchestrate()).start()

    def _update_kill_status(self) -> None:
        """Checks whether all jobs are either succeeded or failed.
        Returns
        -------
        None
        """
        for status in JobStatus:
            if status in [JobStatus.SUCCEEDED, JobStatus.FAILED]:
                pass
            else:
                if self.job_statuses[status]:
                    self.kill_status = False
                    return

        self.kill_status = True

    def _update_psql_entry(self) -> None:
        """Updates a PostgreSQL entry with orchestration status. Assumes
        that a table entry has already been created.
        Returns
        -------
        """
        table_entry = dict()

        for job_status, job_set in self.job_statuses.items():
            table_entry[job_status.value.lower()] = len(job_set)

        print(table_entry)

        update_table_entry(self.psql_conn, "abyss_status",
                           {"abyss_id": self.abyss_id},
                           **table_entry)

    def _orchestrate(self) -> None:
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
        None
        """
        logger.info("STARTING ORCHESTRATION")
        self._predictor_thread.start()
        self._scheduler_thread.start()
        self._prefetcher_thread.start()
        self._prefetcher_poll_thread.start()
        self._funcx_decompress_thread.start()
        self._funcx_crawl_thread.start()
        self._funcx_poll_thread.start()
        self._consolidate_results_thread.start()

        while not self.kill_status:
            time.sleep(5)
            self._update_kill_status()
            self._update_psql_entry()

        self._predictor_thread.join()
        self._scheduler_thread.join()
        self._prefetcher_thread.join()
        self._prefetcher_poll_thread.join()
        self._funcx_decompress_thread.join()
        self._funcx_crawl_thread.join()
        self._funcx_poll_thread.join()
        self._consolidate_results_thread.join()

    # def _orchestrate(self):
    #     unpredicted_set = self.job_statuses[JobStatus.UNPREDICTED]
    #     predicted_set = self.job_statuses[JobStatus.PREDICTED]
    #     scheduled_set = self.job_statuses[JobStatus.SCHEDULED]
    #     prefetching_set = self.job_statuses[JobStatus.PREFETCHING]
    #     prefetched_set = self.job_statuses[JobStatus.PREFETCHED]
    #     failed_set = self.job_statuses[JobStatus.FAILED]
    #     decompressing_set = self.job_statuses[JobStatus.DECOMPRESSING]
    #     decompressed_set = self.job_statuses[JobStatus.DECOMPRESSED]
    #     crawling_set = self.job_statuses[JobStatus.CRAWLING]
    #     succeeded_set = self.job_statuses[JobStatus.SUCCEEDED]
    #
    #     while not self.kill_status:
    #         while unpredicted_set:
    #             job = unpredicted_set.pop()
    #
    #             file_path = job.file_path
    #             compressed_size = job.compressed_size
    #             file_extension = Predictor.get_extension(file_path)
    #
    #             predictor = self.predictors[file_extension]
    #             decompressed_size = predictor.predict(file_path, compressed_size)
    #
    #             job.decompressed_size = decompressed_size
    #             job.status = JobStatus.PREDICTED
    #
    #             predicted_set.add(job)
    #
    #         self.scheduler.schedule_jobs(list(predicted_set))
    #
    #         self.worker_queues = self.scheduler.worker_queues
    #         predicted_set.clear()
    #
    #         for worker_id, worker_queue in self.worker_queues.items():
    #             for _ in range(worker_queue.qsize()):
    #                 job = worker_queue.get()
    #                 job.status = JobStatus.SCHEDULED
    #                 job.worker_id = worker_id
    #                 scheduled_set.add(job)
    #                 worker_queue.put(job)
    #
    #         for worker_id, worker_queue in self.worker_queues.items():
    #             prefetcher = self.prefetchers[worker_id]
    #
    #             while not worker_queue.empty():
    #                 job = worker_queue.get()
    #
    #                 file_path = job.file_path
    #                 worker_id = job.worker_id
    #
    #                 prefetcher.transfer(file_path)
    #
    #                 job.status = JobStatus.PREFETCHING
    #                 job.transfer_path = f"{self.worker_dict[worker_id].transfer_dir}/{file_path}"
    #
    #                 prefetching_set.add(job)
    #                 scheduled_set.remove(job)
    #
    #         prefetched_jobs = []
    #         for job in prefetching_set:
    #             file_path = job.file_path
    #             worker_id = job.worker_id
    #             prefetcher = self.prefetchers[worker_id]
    #
    #             prefetcher_status = prefetcher.get_transfer_status(
    #                 file_path)
    #
    #             if prefetcher_status == PrefetcherStatuses.SUCCEEDED:
    #                 job.status = JobStatus.PREFETCHED
    #                 prefetched_set.add(job)
    #                 prefetched_jobs.append(job)
    #             elif prefetcher_status == PrefetcherStatuses.FAILED:
    #                 print(f"{job.file_path} failed to prefetch")
    #                 job.status = JobStatus.FAILED
    #                 # Potentially add more logic here or in prefetcher to restart failed transfer
    #                 failed_set.add(job)
    #                 prefetched_jobs.append(job)
    #
    #         for job in prefetched_jobs:
    #             prefetching_set.remove(job)
    #
    #         for job in prefetched_set:
    #             file_path = job.transfer_path
    #             worker_id = job.worker_id
    #
    #             worker = self.worker_dict[worker_id]
    #             funcx_task_id = self.funcx_client.run(file_path,
    #                                                   worker.decompress_dir,
    #                                                   endpoint_id=worker.funcx_eid,
    #                                                   function_id=DECOMPRESSOR_FUNCX_UUID)
    #
    #             job.funcx_decompress_id = funcx_task_id
    #             job.status = JobStatus.DECOMPRESSING
    #             decompressing_set.add(job)
    #
    #             time.sleep(1)
    #
    #         prefetched_set.clear()
    #
    #         for job in decompressed_set:
    #             worker_id = job.worker_id
    #
    #             worker = self.worker_dict[worker_id]
    #             funcx_task_id = self.funcx_client.run(
    #                 self.transfer_token,
    #                 job.decompress_path,
    #                 worker.globus_eid,
    #                 "",
    #                 endpoint_id=worker.funcx_eid,
    #                 function_id=GLOBUS_CRAWLER_FUNCX_UUID)
    #             job.funcx_crawl_id = funcx_task_id
    #             job.status = JobStatus.CRAWLING
    #             crawling_set.add(job)
    #
    #             time.sleep(1)
    #
    #         decompressed_set.clear()
    #
    #         decompressed_jobs = []
    #         for job in decompressing_set:
    #             funcx_decompress_id = job.funcx_decompress_id
    #             try:
    #                 result = self.funcx_client.get_result(
    #                     funcx_decompress_id)
    #                 job.decompress_path = result
    #                 job.status = JobStatus.DECOMPRESSED
    #
    #                 decompressed_jobs.append(job)
    #                 decompressed_set.add(job)
    #             # TODO: Handle more exceptions better
    #             except Exception as e:
    #                 if str(e) not in ["waiting-for-ep",
    #                                   "waiting-for-nodes",
    #                                   "waiting-for-launch",
    #                                   "running"]:
    #                     print(
    #                         f"DECOMPRESS FAILED BECAUSE OF {str(e)}")
    #                     decompressed_jobs.append(job)
    #                     failed_set.add(job)
    #                     print(e)
    #                 else:
    #                     time.sleep(1)
    #
    #         for job in decompressed_jobs:
    #             decompressing_set.remove(job)
    #
    #         crawled_jobs = []
    #
    #         for job in crawling_set:
    #             funcx_crawl_id = job.funcx_crawl_id
    #             try:
    #                 result = self.funcx_client.get_result(
    #                     funcx_crawl_id)
    #                 print(result)
    #                 self.crawl_results.put(result)
    #                 job.status = JobStatus.SUCCEEDED
    #
    #                 worker = self.worker_dict[job.worker_id]
    #                 worker.curr_available_space += job.decompressed_size
    #
    #                 crawled_jobs.append(job)
    #                 succeeded_set.add(job)
    #             # TODO: Handle more exceptions better
    #             except Exception as e:
    #                 print(str(e))
    #                 if str(e) not in ["waiting-for-ep",
    #                                   "waiting-for-nodes",
    #                                   "waiting-for-launch",
    #                                   "running"]:
    #                     print(
    #                         f"CRAWL FAILED BECAUSE OF {str(e)}")
    #                     crawled_jobs.append(job)
    #                     failed_set.add(job)
    #                     raise e
    #                 else:
    #                     time.sleep(1)
    #
    #         for job in crawled_jobs:
    #             crawling_set.remove(job)
    #
    #         time.sleep(5)
    #         self._update_kill_status()
    #         self._update_psql_entry()

    def _predict_decompressed_size(self) -> None:
        """Runs decompression size predictions on all files in
        self.compressed_files and then places them in
        self.predicted_files.
        Returns
        -------
        None
        """
        while not self.kill_status:
            unpredicted_set = self.job_statuses[JobStatus.UNPREDICTED]
            predicted_set = self.job_statuses[JobStatus.PREDICTED]

            while unpredicted_set:
                with self._lock:
                    job = unpredicted_set.pop()

                file_path = job.file_path
                compressed_size = job.compressed_size
                file_extension = Predictor.get_extension(file_path)

                predictor = self.predictors[file_extension]
                decompressed_size = predictor.predict(file_path, compressed_size)

                with self._lock:
                    job.decompressed_size = decompressed_size
                    job.status = JobStatus.PREDICTED

                    predicted_set.add(job)

    def _thread_schedule_jobs(self) -> None:
        """Schedules items from self.predicted_files into
        worker queues in self.worker_queues.
        Returns
        -------
        None
        """
        while not self.kill_status:
            predicted_set = self.job_statuses[JobStatus.PREDICTED]
            scheduled_set = self.job_statuses[JobStatus.SCHEDULED]

            with self._lock:
                self.scheduler.schedule_jobs(list(predicted_set))

                self.worker_queues = self.scheduler.worker_queues
                predicted_set.clear()

                for worker_id, worker_queue in self.worker_queues.items():
                    for _ in range(worker_queue.qsize()):
                        job = worker_queue.get()
                        job.status = JobStatus.SCHEDULED
                        job.worker_id = worker_id
                        scheduled_set.add(job)
                        worker_queue.put(job)

    def _thread_prefetch(self) -> None:
        """Places jobs into queue for prefetcher to transfer.
        Returns
        -------
        None
        """
        while not self.kill_status:
            scheduled_set = self.job_statuses[JobStatus.SCHEDULED]
            prefetching_set = self.job_statuses[JobStatus.PREFETCHING]

            for worker_id, worker_queue in self.worker_queues.items():
                prefetcher = self.prefetchers[worker_id]

                while not worker_queue.empty():
                    with self._lock:
                        job = worker_queue.get()

                    file_path = job.file_path
                    worker_id = job.worker_id

                    prefetcher.transfer(file_path)

                    with self._lock:
                        job.status = JobStatus.PREFETCHING
                        job.transfer_path = f"{self.worker_dict[worker_id].transfer_dir}/{file_path}"

                        prefetching_set.add(job)
                        scheduled_set.remove(job)

    def _thread_poll_prefetch(self) -> None:
        """Thread function to poll prefetcher and update
        self.job_statuses.
        Returns
        -------
        None
        """
        while not self.kill_status:
            prefetching_set = self.job_statuses[JobStatus.PREFETCHING]
            prefetched_set = self.job_statuses[JobStatus.PREFETCHED]
            failed_set = self.job_statuses[JobStatus.FAILED]

            prefetched_jobs = []
            with self._lock:
                for job in prefetching_set:
                    file_path = job.file_path
                    worker_id = job.worker_id
                    prefetcher = self.prefetchers[worker_id]

                    prefetcher_status = prefetcher.get_transfer_status(file_path)

                    if prefetcher_status == PrefetcherStatuses.SUCCEEDED:
                        job.status = JobStatus.PREFETCHED
                        prefetched_set.add(job)
                        prefetched_jobs.append(job)
                    elif prefetcher_status == PrefetcherStatuses.FAILED:
                        print(f"{job.file_path} failed to prefetch")
                        job.status = JobStatus.FAILED
                        # Potentially add more logic here or in prefetcher to restart failed transfer
                        failed_set.add(job)
                        prefetched_jobs.append(job)

                for job in prefetched_jobs:
                    prefetching_set.remove(job)

            time.sleep(10)

    # TODO: Consolidate this and _thread_funcx_crawl into one function
    def _thread_funcx_decompress(self) -> None:
        """Thread function to submit decompression functions to funcX.
        Returns
        -------
        None
        """
        while not self.kill_status:
            prefetched_set = self.job_statuses[JobStatus.PREFETCHED]
            decompressing_set = self.job_statuses[JobStatus.DECOMPRESSING]

            with self._lock:
                for job in prefetched_set:
                    file_path = job.transfer_path
                    worker_id = job.worker_id

                    worker = self.worker_dict[worker_id]
                    funcx_task_id = self.funcx_client.run(file_path,
                                                          worker.decompress_dir,
                                                          endpoint_id=worker.funcx_eid,
                                                          function_id=DECOMPRESSOR_FUNCX_UUID)

                    job.funcx_decompress_id = funcx_task_id
                    job.status = JobStatus.DECOMPRESSING
                    decompressing_set.add(job)

                    time.sleep(1)

                prefetched_set.clear()

    def _thread_funcx_crawl(self) -> None:
        """Thread function to submit crawl functions to funcX.
        Returns
        -------
        None
        """
        while not self.kill_status:
            decompressed_set = self.job_statuses[JobStatus.DECOMPRESSED]
            crawling_set = self.job_statuses[JobStatus.CRAWLING]

            print("WAITING FOR LOCK")
            with self._lock:
                print("ACQUIRED LOCK")
                for job in decompressed_set:
                    worker_id = job.worker_id

                    worker = self.worker_dict[worker_id]
                    funcx_task_id = self.funcx_client.run(self.transfer_token,
                                                          job.decompress_path,
                                                          worker.globus_eid,
                                                          "",
                                                          endpoint_id=worker.funcx_eid,
                                                          function_id=GLOBUS_CRAWLER_FUNCX_UUID)
                    print(funcx_task_id)
                    job.funcx_crawl_id = funcx_task_id
                    job.status = JobStatus.CRAWLING
                    crawling_set.add(job)

                    time.sleep(1)
                decompressed_set.clear()

    def _thread_funcx_poll(self) -> None:
        """Thread function to poll funcX for results.
        Returns
        -------
        None
        """
        while not self.kill_status:
            decompressing_set = self.job_statuses[JobStatus.DECOMPRESSING]
            decompressed_set = self.job_statuses[JobStatus.DECOMPRESSED]
            crawling_set = self.job_statuses[JobStatus.CRAWLING]
            succeeded_set = self.job_statuses[JobStatus.SUCCEEDED]
            failed_set = self.job_statuses[JobStatus.FAILED]

            decompressed_jobs = []

            with self._lock:
                for job in decompressing_set:
                    funcx_decompress_id = job.funcx_decompress_id
                    try:
                        result = self.funcx_client.get_result(funcx_decompress_id)
                        job.decompress_path = result
                        job.status = JobStatus.DECOMPRESSED

                        decompressed_jobs.append(job)
                        decompressed_set.add(job)
                    # TODO: Handle more exceptions better
                    except Exception as e:
                        if str(e) not in ["waiting-for-ep", "waiting-for-nodes",
                                          "waiting-for-launch", "running"]:
                            print(f"DECOMPRESS FAILED BECAUSE OF {str(e)}")
                            decompressed_jobs.append(job)
                            failed_set.add(job)
                            print(e)
                        else:
                            time.sleep(3)

                for job in decompressed_jobs:
                    decompressing_set.remove(job)

                crawled_jobs = []

                for job in crawling_set:
                    funcx_crawl_id = job.funcx_crawl_id
                    try:
                        result = self.funcx_client.get_result(funcx_crawl_id)
                        print(result)
                        self.crawl_results.put(result)
                        job.status = JobStatus.SUCCEEDED

                        worker = self.worker_dict[job.worker_id]
                        worker.curr_available_space += job.decompressed_size

                        crawled_jobs.append(job)
                        succeeded_set.add(job)
                    # TODO: Handle more exceptions better
                    except Exception as e:
                        print(str(e))
                        if str(e) not in ["waiting-for-ep", "waiting-for-nodes",
                                      "waiting-for-launch", "running"]:
                            print(
                                f"CRAWL FAILED BECAUSE OF {str(e)}")
                            crawled_jobs.append(job)
                            failed_set.add(job)
                            raise e
                        else:
                            time.sleep(3)

                for job in crawled_jobs:
                    crawling_set.remove(job)

    def _thread_consolidate_crawl_results(self) -> None:
        """Thread function to consolidate crawl results and push to SQS.
        Returns
        -------
        None
        """
        while not self.kill_status:
            while not(self.crawl_results.empty()):
                crawl_result = self.crawl_results.get()
                consolidated_metadata = {
                    "compressed_path": crawl_result["root_path"],
                    "metadata": crawl_result["metadata"],
                    "files": [],
                    "decompressed_size": 0
                }

                for file_metadata in crawl_result["metadata"]:
                    file_path = file_metadata["path"]
                    file_size = file_metadata["metadata"]["physical"]["size"]

                    consolidated_metadata["files"].append(file_path)
                    consolidated_metadata["decompressed_size"] += file_size

                    if is_compressed(file_path):
                        # TODO: Implement task resubmission
                        pass

                put_message(self.sqs_conn, consolidated_metadata,
                            self.sqs_queue_name)


if __name__ == "__main__":
    import pandas as pd
    import os
    from abyss.utils.psql_utils import read_db_config_file, create_connection
    from abyss.utils.sqs_utils import create_sqs_connection, read_sqs_config_file

    PROJECT_ROOT = os.path.realpath(os.path.dirname(__file__)) + "/"
    print(PROJECT_ROOT)
    deep_blue_crawl_df = pd.read_csv("/Users/ryan/Documents/CS/abyss/data/deep_blue_crawl.csv")
    filtered_files = deep_blue_crawl_df[deep_blue_crawl_df.extension == "zip"].sort_values(by=["size_bytes"]).iloc[1:100]


    workers = [{"globus_eid": "3f487096-811c-11eb-a933-81bbe47059f4",
                "funcx_eid": "99da411c-92b4-4b44-a86c-dc4abb5cbe0a",
                "max_available_space": 30*10**9,
                "transfer_dir": "/home/tskluzac/ryan/deep_blue_data",
                "decompress_dir": "/home/tskluzac/ryan/results"}]

    compressed_files = [{"file_path": x[0], "compressed_size": x[1]} for _, x in filtered_files.iterrows()]
    transfer_token = 'Ago2vz44pB9bm58XvwVVWvpv3QJxe3W35V9oN1oPgPyao5NwkoHgCybQd7NMemmEBgVxBn095J0grGCKqEop2f1605'
    abyss_id = str(uuid.uuid4())

    psql_conn = create_connection(read_db_config_file("/Users/ryan/Documents/CS/abyss/database.ini"))
    sqs_conn = create_sqs_connection(**read_sqs_config_file("/Users/ryan/Documents/CS/abyss/sqs.ini"))

    orchestrator = AbyssOrchestrator(abyss_id,"4f99675c-ac1f-11ea-bee8-0e716405a293",
                                     transfer_token, compressed_files,
                                     workers, psql_conn, sqs_conn)
    import time

    t0 = time.time()
    orchestrator._orchestrate()
    print(time.time() - t0)
