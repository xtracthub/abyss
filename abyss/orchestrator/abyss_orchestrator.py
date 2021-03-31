import json
import logging
import threading
import uuid
from queue import Queue
from typing import Dict, List

from funcx import FuncXClient

from abyss.crawlers.globus_crawler.globus_crawler import GLOBUS_CRAWLER_FUNCX_UUID
from abyss.crawlers.local_crawler.local_crawler import \
    LOCAL_CRAWLER_FUNCX_UUID
from abyss.decompressors import DECOMPRESSOR_FUNCX_UUID
from abyss.orchestrator.job import Job, JobStatus
from abyss.orchestrator.worker import Worker
from abyss.predictors import FILE_PREDICTOR_MAPPING
from abyss.predictors.predictor import Predictor
from abyss.prefetcher.globus_prefetcher import GlobusPrefetcher, \
    PrefetcherStatuses
from abyss.schedulers.scheduler import Scheduler
from abyss.utils.psql_utils import update_table_entry
from abyss.utils.aws_utils import s3_upload_file
from abyss.utils.error_utils import is_non_critical_funcx_error, is_critical_decompression_error, is_critical_oom_error

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
                 psql_conn, s3_conn, grouper="", batcher="mmd",
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
            worker.worker_id = "4c0f8eb8-6363-4f34-a6e0-4fee6d2621f3"
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
                                     [Queue() for _ in range(len(JobStatus))]))
        unpredicted_set = self.job_statuses[JobStatus.UNPREDICTED]
        for compressed_file in compressed_files:
            job = Job.from_dict(compressed_file)
            job.status = JobStatus.UNPREDICTED
            job.file_id = f"{str(uuid.uuid4())}"
            # job.file_extension = os.path.splitext(job.file_path)[1][1:]
            unpredicted_set.put(job)

        self.scheduler = Scheduler(batcher, dispatcher,
                                   list(self.worker_dict.values()), [])
        self.worker_queues = dict()

        self.psql_conn = psql_conn
        self.abyss_metadata = []
        self.s3_conn = s3_conn
        # self.sqs_conn = sqs_conn
        # self.sqs_queue_name = f"abyss_{self.abyss_id}"
        # make_queue(self.sqs_conn, self.sqs_queue_name)

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
        self.thread_statuses = {
            "predictor_thread": True,
            "scheduler_thread": True,
            "prefetcher_thread": True,
            "prefetcher_poll_thread": True,
            "funcx_decompress_thread": True,
            "funcx_crawl_thread": True,
            "funcx_poll_thread": True,
            "consolidate_results_thread": True
        }

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
                if not self.job_statuses[status].empty():
                    self.kill_status = False
                    return

        print(self.thread_statuses)
        for status in self.thread_statuses.values():
            if status:
                self.kill_status = False
                return

        self.kill_status = True
        print(f"KILL STATUS {self.kill_status}")

    def _update_psql_entry(self) -> None:
        """Updates a PostgreSQL entry with orchestration status. Assumes
        that a table entry has already been created.
        Returns
        -------
        """
        table_entry = dict()

        for job_status, job_queue in self.job_statuses.items():
            table_entry[job_status.value.lower()] = job_queue.qsize()

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

        t0 = time.time()
        while not self.kill_status:
            time.sleep(1)
            self._update_kill_status()
            self._update_psql_entry()
            print(f"ELAPSED: {time.time() - t0}")

        self._predictor_thread.join()
        self._scheduler_thread.join()
        self._prefetcher_thread.join()
        self._prefetcher_poll_thread.join()
        self._funcx_decompress_thread.join()
        self._funcx_crawl_thread.join()
        self._funcx_poll_thread.join()
        self._consolidate_results_thread.join()

        metadata_file_path = os.path.join("/tmp", f"{self.abyss_id}.txt")

        with open(metadata_file_path, "w") as f:
            f.writelines([json.dumps(metadata) for metadata in self.abyss_metadata])

        s3_upload_file(self.s3_conn, "xtract-abyss", metadata_file_path,
                       f"{self.abyss_id}.txt")

        os.remove(metadata_file_path)

    def _predict_decompressed_size(self) -> None:
        """Runs decompression size predictions on all files in
        self.compressed_files and then places them in
        self.predicted_files.
        Returns
        -------
        None
        """
        while not self.kill_status:
            unpredicted_queue = self.job_statuses[JobStatus.UNPREDICTED]
            predicted_queue = self.job_statuses[JobStatus.PREDICTED]

            while not unpredicted_queue.empty():
                self.thread_statuses["predictor_thread"] = True
                job = unpredicted_queue.get()
                print(f"{job.file_path} PREDICTING")

                for job_node in job.bfs_iterator(include_root=True):
                    if job_node.status == JobStatus.UNPREDICTED:
                        file_path = job_node.file_path
                        compressed_size = job_node.compressed_size
                        file_extension = Predictor.get_extension(file_path)

                        predictor = self.predictors[file_extension]
                        decompressed_size = predictor.predict(file_path, compressed_size)

                        with self._lock:
                            job_node.decompressed_size = decompressed_size
                            job_node.status = JobStatus.PREDICTED

                predicted_queue.put(job)

            self.thread_statuses["predictor_thread"] = False

    def _thread_schedule_jobs(self) -> None:
        """Schedules items from self.predicted_files into
        worker queues in self.worker_queues.
        Returns
        -------
        None
        """
        while not self.kill_status:
            predicted_queue = self.job_statuses[JobStatus.PREDICTED]
            scheduled_queue = self.job_statuses[JobStatus.SCHEDULED]

            with self._lock:
                predicted_list = []
                while not predicted_queue.empty():
                    self.thread_statuses["scheduler_thread"] = True
                    job = predicted_queue.get()
                    print(f"{job.file_path} SCHEDULING")
                    job.calculate_total_size()
                    predicted_list.append(job)

                self.scheduler.schedule_jobs(predicted_list)

                self.worker_queues = self.scheduler.worker_queues

                for job in predicted_list:
                    for job_node in job.bfs_iterator(include_root=True):
                        if job_node.status == JobStatus.PREDICTED:
                            job_node.status = JobStatus.SCHEDULED
                    scheduled_queue.put(job)
                self.thread_statuses["scheduler_thread"] = False

    def _thread_prefetch(self) -> None:
        """Places jobs into queue for prefetcher to transfer.
        Returns
        -------
        None
        """
        while not self.kill_status:
            scheduled_queue = self.job_statuses[JobStatus.SCHEDULED]
            prefetching_queue = self.job_statuses[JobStatus.PREFETCHING]

            for worker_id, worker_queue in self.worker_queues.items():
                prefetcher = self.prefetchers[worker_id]

                while not worker_queue.empty():
                    self.thread_statuses["prefetcher_thread"] = True
                    job = worker_queue.get()
                    print(f"{job.file_path} PREFETCHING")

                    file_path = job.file_path
                    worker_id = job.worker_id

                    prefetcher.transfer(file_path, job.file_id)

                    with self._lock:
                        job.transfer_path = f"{self.worker_dict[worker_id].transfer_dir}/{job.file_id}"

                        for job_node in job.bfs_iterator(include_root=True):
                            if job_node.status == JobStatus.SCHEDULED:
                                job_node.status = JobStatus.PREFETCHING

                        prefetching_queue.put(job)
                        scheduled_queue.get(job)
            self.thread_statuses["prefetcher_thread"] = False

    def _thread_poll_prefetch(self) -> None:
        """Thread function to poll prefetcher and update
        self.job_statuses.
        Returns
        -------
        None
        """
        while not self.kill_status:
            prefetching_queue = self.job_statuses[JobStatus.PREFETCHING]
            prefetched_queue = self.job_statuses[JobStatus.PREFETCHED]
            failed_queue = self.job_statuses[JobStatus.FAILED]

            for _ in range(prefetching_queue.qsize()):
                self.thread_statuses["prefetcher_poll_thread"] = True
                job = prefetching_queue.get()
                print(f"{job.file_path} POLL PREFETCH")
                file_path = job.file_path
                worker_id = job.worker_id
                prefetcher = self.prefetchers[worker_id]

                print("GETTING STATUS")
                prefetcher_status = prefetcher.get_transfer_status(file_path)
                print(prefetcher_status)
                if prefetcher_status == PrefetcherStatuses.SUCCEEDED:
                    for job_node in job.bfs_iterator(include_root=True):
                        if job_node.status == JobStatus.PREFETCHING:
                            job_node.status = JobStatus.PREFETCHED

                    prefetched_queue.put(job)
                elif prefetcher_status == PrefetcherStatuses.FAILED:
                    for job_node in job.bfs_iterator(include_root=True):
                        if job_node.status == JobStatus.PREFETCHING:
                            job_node.status = JobStatus.FAILED
                    print(f"{job.file_path} FAILED TO PREFETCH")
                    # Potentially add more logic here or in prefetcher to restart failed transfer
                    failed_queue.put(job)
                else:
                    prefetching_queue.put(job)

            self.thread_statuses["prefetcher_poll_thread"] = False
            time.sleep(10)

    # TODO: Consolidate this and _thread_funcx_crawl into one function
    def _thread_funcx_decompress(self) -> None:
        """Thread function to submit decompression functions to funcX.
        Returns
        -------
        None
        """
        while not self.kill_status:
            prefetched_queue = self.job_statuses[JobStatus.PREFETCHED]
            decompressing_queue = self.job_statuses[JobStatus.DECOMPRESSING]

            while not prefetched_queue.empty():
                self.thread_statuses["funcx_decompress_thread"] = True
                job = prefetched_queue.get()
                print(f"{job.file_path} DECOMPRESSING")
                print(f"{job.file_path} {Job.to_dict(job)}")
                job_dict = Job.to_dict(job)
                worker_id = job.worker_id

                worker = self.worker_dict[worker_id]
                funcx_task_id = self.funcx_client.run(job_dict,
                                                      worker.decompress_dir,
                                                      endpoint_id=worker.funcx_eid,
                                                      function_id=DECOMPRESSOR_FUNCX_UUID)
                for job_node in job.bfs_iterator(include_root=True):
                    job_node.funcx_decompress_id = funcx_task_id
                    if job_node.status == JobStatus.PREFETCHED:
                        job_node.status = JobStatus.DECOMPRESSING
                decompressing_queue.put(job)

                time.sleep(1)

            self.thread_statuses["funcx_decompress_thread"] = False

    def _thread_funcx_crawl(self) -> None:
        """Thread function to submit crawl functions to funcX.
        Returns
        -------
        None
        """
        while not self.kill_status:
            decompressed_queue = self.job_statuses[JobStatus.DECOMPRESSED]
            crawling_queue = self.job_statuses[JobStatus.CRAWLING]

            while not decompressed_queue.empty():
                self.thread_statuses["funcx_crawl_thread"] = True
                job = decompressed_queue.get()
                print(f"{job.file_path} CRAWLING")
                job_dict = Job.to_dict(job)
                print(f"{job.file_path} {job_dict}")
                worker_id = job.worker_id

                worker = self.worker_dict[worker_id]
                funcx_task_id = self.funcx_client.run(job_dict,
                                                      "",
                                                      endpoint_id=worker.funcx_eid,
                                                      function_id=LOCAL_CRAWLER_FUNCX_UUID)

                for job_node in job.bfs_iterator(include_root=True):
                    job_node.funcx_crawl_id = funcx_task_id
                    if job_node.status == JobStatus.DECOMPRESSED:
                        job_node.status = JobStatus.CRAWLING

                crawling_queue.put(job)

                time.sleep(1)

            self.thread_statuses["funcx_crawl_thread"] = False

    def _thread_funcx_poll(self) -> None:
        """Thread function to poll funcX for results.
        Returns
        -------
        None
        """
        while not self.kill_status:
            decompressing_queue = self.job_statuses[JobStatus.DECOMPRESSING]
            decompressed_queue = self.job_statuses[JobStatus.DECOMPRESSED]
            crawling_queue = self.job_statuses[JobStatus.CRAWLING]
            consolidating_queue = self.job_statuses[JobStatus.CONSOLIDATING]
            failed_queue = self.job_statuses[JobStatus.FAILED]

            for _ in range(decompressing_queue.qsize()):
                self.thread_statuses["funcx_poll_thread"] = True
                job = decompressing_queue.get()
                print(f"{job.file_path} POLLING DECOMPRESS")
                print(f"POLLING DECOMPRESS {job.file_path} {Job.to_dict(job)}")
                funcx_decompress_id = job.funcx_decompress_id
                try:
                    result = self.funcx_client.get_result(funcx_decompress_id)
                    job = Job.from_dict(result)
                    print(f"{job.file_path} {Job.to_dict(job)} COMPLETED DECOMPRESS")
                    print(f"{job.file_path} COMPLETED DECOMPRESS")

                    for job_node in job.bfs_iterator(include_root=True):
                        if job_node.status == JobStatus.DECOMPRESSING:
                            job_node.status = JobStatus.DECOMPRESSED

                    worker = self.worker_dict[job.worker_id]
                    worker.curr_available_space += job.compressed_size

                    decompressed_queue.put(job)
                except Exception as e:
                    if is_non_critical_funcx_error(e):
                        decompressing_queue.put(job)
                    elif is_critical_decompression_error(e):
                        for job_node in job.bfs_iterator(include_root=True):
                            if job_node.status == JobStatus.DECOMPRESSING:
                                job_node.status = JobStatus.FAILED
                        failed_queue.put(job)
                    elif is_critical_oom_error(e):
                        #TODO: Do some reprocessing here
                        pass
                    else:
                        failed_queue.put(job)
                    print("RIPPPP")

                time.sleep(5)

            for _ in range(crawling_queue.qsize()):
                self.thread_statuses["funcx_poll_thread"] = True
                job = crawling_queue.get()
                print(f"{job.file_path} POLLING CRAWL")
                print(
                    f"POLLING CRAWL {job.file_path} {Job.to_dict(job)}")
                funcx_crawl_id = job.funcx_crawl_id
                try:
                    result = self.funcx_client.get_result(funcx_crawl_id)
                    job = Job.from_dict(result)
                    print(
                        f"{job.file_path} {Job.to_dict(job)} COMPLETED CRAWL")
                    print(f"{job.file_path} COMPLETED CRAWL")

                    for job_node in job.bfs_iterator(include_root=True):
                        if job_node.status == JobStatus.CRAWLING:
                            job_node.status = JobStatus.CONSOLIDATING

                    worker = self.worker_dict[job.worker_id]
                    #TODO: Check if this is correct
                    worker.curr_available_space += (job.total_size - job.decompressed_size)

                    consolidating_queue.put(job)
                except Exception as e:
                    print(str(e))
                    if is_non_critical_funcx_error(e):
                        crawling_queue.put(job)
                    elif is_critical_decompression_error(e):
                        for job_node in job.bfs_iterator(
                                include_root=True):
                            if job_node.status == JobStatus.CRAWLING:
                                job_node.status = JobStatus.FAILED
                        failed_queue.put(job)
                    elif is_critical_oom_error(e):
                        # TODO: Do some reprocessing here
                        pass
                    else:
                        failed_queue.put(job)
                    print("RIPPPP")

                time.sleep(5)

            self.thread_statuses["funcx_poll_thread"] = False

    def _thread_consolidate_crawl_results(self) -> None:
        """Thread function to consolidate crawl results and push to SQS.
        Returns
        -------
        None
        """
        while not self.kill_status:
            unpredicted_queue = self.job_statuses[JobStatus.UNPREDICTED]
            consolidating_queue = self.job_statuses[JobStatus.CONSOLIDATING]
            succeeded_queue = self.job_statuses[JobStatus.SUCCEEDED]

            while not consolidating_queue.empty():
                self.thread_statuses["consolidate_results_thread"] = True
                job = consolidating_queue.get()

                resubmit_task = False
                for job_node in job.bfs_iterator(include_root=True):
                    root_path = job_node.metadata["root_path"]
                    for file_path, file_metadata in job_node.metadata["metadata"].items():
                        file_size = file_metadata["physical"]["size"]
                        is_compressed = file_metadata["physical"]["is_compressed"]

                        child_file_path = os.path.join(root_path, file_path)

                        if is_compressed:
                            if child_file_path in job_node.child_jobs:
                                break
                            else:
                                child_job = Job(
                                    file_path=child_file_path,
                                    file_id=f"{str(uuid.uuid4())}",
                                    compressed_size=file_size,
                                    status=JobStatus.UNPREDICTED
                                )
                                job_node.child_jobs[child_file_path] = child_job
                                resubmit_task = True

                if resubmit_task:
                    unpredicted_queue.put(job)
                    continue

                consolidated_metadata = job.consolidate_metadata()
                self.abyss_metadata.append(consolidated_metadata)

                for job_node in job.bfs_iterator(include_root=True):
                    if job_node.status == JobStatus.CONSOLIDATING:
                        job_node.status = JobStatus.SUCCEEDED

                succeeded_queue.put(job)

            self.thread_statuses["consolidate_results_thread"] = False


if __name__ == "__main__":
    import pandas as pd
    import os
    from abyss.utils.psql_utils import read_db_config_file, create_connection
    from abyss.utils.aws_utils import create_s3_connection, read_aws_config_file

    PROJECT_ROOT = os.path.realpath(os.path.dirname(__file__)) + "/"
    print(PROJECT_ROOT)
    deep_blue_crawl_df = pd.read_csv("/Users/ryan/Documents/CS/abyss/data/deep_blue_crawl.csv")
    filtered_files = deep_blue_crawl_df[deep_blue_crawl_df.extension == "gz"].sort_values(by=["size_bytes"]).iloc[1:2]
    print(sum(filtered_files.size_bytes))


    workers = [{"globus_eid": "3f487096-811c-11eb-a933-81bbe47059f4",
                "funcx_eid": "66dab10e-d323-41e1-8f4a-4bfc3204357e",
                "max_available_space": 30*10**9,
                "transfer_dir": "/home/tskluzac/ryan/deep_blue_data",
                "decompress_dir": "/home/tskluzac/ryan/results"}]

    compressed_files = [{"file_path": x[0], "compressed_size": x[1]} for _, x in filtered_files.iterrows()]
    transfer_token = 'AgEg7okMz5DE1d9Ee5Bam6jQG2V5X0b1B6z2K7v5kXJk0blgBzFwCwN8evOKzYG7w78WGX9JwJlgexhyrMze0Ip5gz'
    abyss_id = str(uuid.uuid4())
    print(abyss_id)

    psql_conn = create_connection(read_db_config_file("/Users/ryan/Documents/CS/abyss/database.ini"))
    sqs_conn = create_s3_connection(**read_aws_config_file("/Users/ryan/Documents/CS/abyss/sqs.ini"))

    orchestrator = AbyssOrchestrator(abyss_id,"4f99675c-ac1f-11ea-bee8-0e716405a293",
                                     transfer_token, compressed_files,
                                     workers, psql_conn, sqs_conn)

    # d = {'file_path': '/UMich/download/DeepBlueData_79407x76d/fig01.tar.gz', 'file_id': '6bc77252-1a2f-40e9-9b77-a3c23cb32f79', 'compressed_size': 38664, 'decompressed_size': 106857, 'total_size': 145521, 'worker_id': '4c0f8eb8-6363-4f34-a6e0-4fee6d2621f3', 'transfer_path': '/home/tskluzac/ryan/deep_blue_data/6bc77252-1a2f-40e9-9b77-a3c23cb32f79', 'decompress_path': '/home/tskluzac/ryan/results/6bc77252-1a2f-40e9-9b77-a3c23cb32f79', 'funcx_decompress_id': None, 'funcx_crawl_id': None, 'status': 'DECOMPRESSED', 'metadata': {}, 'child_jobs': {}}
    # orchestrator.job_statuses[JobStatus.DECOMPRESSED].put(Job.from_dict(d))

    import time

    t0 = time.time()
    orchestrator._orchestrate()
    print(time.time() - t0)
    """
    TODO: Check out why /UMich/download/DeepBlueData_pv63g053w/repro_200k_annotations.tar.gz is not 
    producing any metadata. There should be ~800mb worth of files after decompressing tar
    """
