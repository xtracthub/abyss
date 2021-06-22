import logging
import logging
import threading
import uuid
from queue import Queue
from typing import Dict, List

from funcx import FuncXClient

from abyss.orchestrator.job import Job, JobStatus
from abyss.orchestrator.worker import Worker
from abyss.predictors import FILE_PREDICTOR_MAPPING
from abyss.predictors.predictor import Predictor
from abyss.prefetcher.globus_prefetcher import GlobusPrefetcher, \
    PrefetcherStatuses
from abyss.schedulers.scheduler import Scheduler
from abyss.utils.error_utils import is_non_critical_funcx_error
from abyss.utils.funcx_functions import LOCAL_CRAWLER_FUNCX_UUID, \
    DECOMPRESSOR_FUNCX_UUID, PROCESS_HEADER_FUNCX_UUID
from abyss.utils.psql_utils import update_table_entry

REQUIRED_ORCHESTRATOR_PARAMETERS = [
            ("globus_source_eid", str),
            ("transfer_token", str),
            ("compressed_files", list),
            ("worker_params", list)
        ]

logger = logging.getLogger(__name__)
f_handler = logging.FileHandler('/Users/ryan/Documents/CS/abyss/abyss/orchestrator/file.log')
f_handler.setLevel(logging.ERROR)
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)


class AbyssOrchestrator:
    def __init__(self, abyss_id: str, globus_source_eid: str, transfer_token: str,
                 compressed_files: List[Dict], worker_params: List[Dict],
                 psql_conn, s3_conn, grouper="", batcher="mmd",
                 dispatcher="fifo", prediction_mode="ml"):
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
        prediction_mode: str
            Mode of prediction to use to predict decompressed file size.
            "ml" to use machine learning method or "header" to use
            metadata stored in the header of compressed files (where
            possible).
        """
        self.abyss_id = abyss_id
        self.globus_source_eid = globus_source_eid
        self.transfer_token = transfer_token
        self.grouper = grouper
        self.prediction_mode = prediction_mode

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
            file_predictor.load_models()
            self.predictors[file_type] = file_predictor

        self.job_statuses = dict(zip([x for x in JobStatus],
                                     [Queue() for _ in range(len(JobStatus))]))
        unpredicted_set = self.job_statuses[JobStatus.UNPREDICTED]
        for compressed_file in compressed_files:
            job = Job.from_dict(compressed_file)
            job.status = JobStatus.UNPREDICTED
            job.file_id = f"{str(uuid.uuid4())}"
            unpredicted_set.put(job)

        self.scheduler = Scheduler(batcher, dispatcher,
                                   list(self.worker_dict.values()), [])
        self.worker_queues = dict()

        self.psql_conn = psql_conn
        self.abyss_metadata = []
        self.s3_conn = s3_conn

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

        for status in self.thread_statuses.values():
            if status:
                self.kill_status = False
                return

        self.kill_status = True
        logger.error(f"KILL STATUS {self.kill_status}")

    def _update_psql_entry(self) -> None:
        """Updates a PostgreSQL entry with orchestration status. Assumes
        that a table entry has already been created.
        Returns
        -------
        """
        table_entry = dict()

        for job_status, job_queue in self.job_statuses.items():
            table_entry[job_status.value.lower()] = job_queue.qsize()

        logger.error(table_entry)
        logger.error(self.thread_statuses)

        for worker_id, worker in self.worker_dict.items():
            logger.error(f"{worker.worker_id} has {worker.curr_available_space}")

        print(table_entry)
        print(self.thread_statuses)

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
        logger.error("STARTING ORCHESTRATION")
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
            logger.error(f"ELAPSED: {time.time() - t0}")

        self._predictor_thread.join()
        self._scheduler_thread.join()
        self._prefetcher_thread.join()
        self._prefetcher_poll_thread.join()
        self._funcx_decompress_thread.join()
        self._funcx_crawl_thread.join()
        self._funcx_poll_thread.join()
        self._consolidate_results_thread.join()

        # for metadata in self.abyss_metadata:
        #     logger.error(metadata)

        # metadata_file_path = os.path.join("/tmp", f"{self.abyss_id}.txt")
        #
        # with open(metadata_file_path, "w") as f:
        #     f.writelines([json.dumps(metadata) for metadata in self.abyss_metadata])

        # s3_upload_file(self.s3_conn, "xtract-abyss", metadata_file_path,
        #                f"{self.abyss_id}.txt")

        # os.remove(metadata_file_path)

    def _unpredicted_preprocessing(self) -> None:
        """Determines whether to use machine learning or file headers
        for decompressed size prediction and places jobs into respective
        queues.

        Returns
        -------
        None
        """
        while not self.kill_status:
            unpredicted_queue = self.job_statuses[JobStatus.UNPREDICTED]
            unpredicted_predict_queue = self.job_statuses[JobStatus.UNPREDICTED_PREDICT]
            unpredicted_schedule_queue = self.job_statuses[JobStatus.UNPREDICTED_SCHEDULE]

            while not unpredicted_queue.empty():
                self.thread_statuses["unpredicted_preprocessing_thread"] = True
                job = unpredicted_queue.get()

                # If a file is recursively compressed we will use machine learning to predict the file size.
                # We only use file headers if the compressed file is directly stored on our storage source.
                if self.prediction_mode == "ml" or job.status != JobStatus.UNPREDICTED:
                    job.status = JobStatus.UNPREDICTED_PREDICT
                    unpredicted_predict_queue.put(job)
                elif self.prediction_mode == "header":
                    if job.file_path.endswith(".zip") or job.file_path.endswith(".tar"):
                        job.status = JobStatus.UNPREDICTED_SCHEDULE
                        unpredicted_schedule_queue.put(job)
                else:
                    raise ValueError(f"Unknown prediction mode \"{self.prediction_mode}\"")

                self.thread_statuses["unpredicted_preprocessing_thread"] = False

    def _predict_decompressed_size(self) -> None:
        """Runs decompression size predictions on all files in
        self.compressed_files and then places them in
        self.predicted_files.

        Returns
        -------
        None
        """
        while not self.kill_status:
            unpredicted_queue = self.job_statuses[JobStatus.UNPREDICTED_PREDICT]
            predicted_queue = self.job_statuses[JobStatus.PREDICTED]

            while not unpredicted_queue.empty():
                self.thread_statuses["predictor_thread"] = True
                job = unpredicted_queue.get()
                logger.error(f"{job.file_path} PREDICTING")

                for job_node in job.bfs_iterator(include_root=True):
                    if job_node.status in [JobStatus.UNPREDICTED, JobStatus.UNPREDICTED_PREDICT]:
                        file_path = job_node.file_path
                        file_extension = Predictor.get_extension(file_path)

                        predictor = self.predictors[file_extension]

                        if job_node.decompressed_size:
                            decompressed_size = predictor.repredict(job_node.decompressed_size)
                        else:
                            compressed_size = job_node.compressed_size
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
            unpredicted_schedule_queue = self.job_statuses[JobStatus.UNPREDICTED_SCHEDULE]
            unpredicted_scheduled_queue = self.job_statuses[JobStatus.UNPREDICTED_SCHEDULED]
            scheduled_queue = self.job_statuses[JobStatus.SCHEDULED]
            failed_queue = self.job_statuses[JobStatus.FAILED]

            with self._lock:
                predicted_list = []
                while not predicted_queue.empty():
                    self.thread_statuses["scheduler_thread"] = True
                    job = predicted_queue.get()
                    logger.error(f"{job.file_path} SCHEDULING")
                    job.calculate_total_size()
                    predicted_list.append(job)

                while not unpredicted_schedule_queue.empty():
                    self.thread_statuses["scheduler_thread"] = True
                    job = unpredicted_schedule_queue.get()
                    logger.error(f"{job.file_path} UNPREDICTED SCHEDULING")
                    job.calculate_total_size()
                    predicted_list.append(job)

                self.scheduler.schedule_jobs(predicted_list)

                self.worker_queues = self.scheduler.worker_queues
                failed_jobs = self.scheduler.failed_jobs

                for job in predicted_list:
                    print(Job.to_dict(job))
                    for job_node in job.bfs_iterator(include_root=True):
                        if job in failed_jobs:
                            job_node.status = JobStatus.FAILED
                            job_node.eror = "Could not schedule"
                        elif job_node.status == JobStatus.PREDICTED:
                            job_node.status = JobStatus.SCHEDULED
                        elif job_node.status == JobStatus.UNPREDICTED_SCHEDULE:
                            job_node.status = JobStatus.UNPREDICTED_SCHEDULED

                    if job not in failed_jobs:
                        if job_node.status == JobStatus.SCHEDULED:
                            scheduled_queue.put(job)
                        elif job_node.status == JobStatus.UNPREDICTED_SCHEDULED:
                            unpredicted_scheduled_queue.put(job)
                    else:
                        logger.error(f"{job.file_path} placed into failed")
                        failed_queue.put(job)
                self.thread_statuses["scheduler_thread"] = False

    def _thread_prefetch(self) -> None:
        """Places jobs into queue for prefetcher to transfer.
        Returns
        -------
        None
        """
        while not self.kill_status:
            scheduled_queue = self.job_statuses[JobStatus.SCHEDULED]
            unpredicted_scheduled_queue = self.job_statuses[JobStatus.UNPREDICTED_SCHEDULED]
            prefetching_queue = self.job_statuses[JobStatus.PREFETCHING]
            unpredicted_prefetching_queue = self.job_statuses[JobStatus.UNPREDICTED_PREFETCHING]

            with self._lock:
                for worker_id, worker_queue in self.worker_queues.items():
                    prefetcher = self.prefetchers[worker_id]

                    while len(worker_queue):
                        self.thread_statuses["prefetcher_thread"] = True
                        job = worker_queue.popleft()
                        logger.error(f"{job.file_path} PREFETCHING")

                        file_path = job.file_path
                        worker_id = job.worker_id

                        prefetcher.transfer(file_path, job.file_id)

                        job.transfer_path = f"{self.worker_dict[worker_id].transfer_dir}/{job.file_id}"

                        for job_node in job.bfs_iterator(include_root=True):
                            if job_node.status == JobStatus.SCHEDULED:
                                job_node.status = JobStatus.PREFETCHING
                            elif job_node.status == JobStatus.UNPREDICTED_SCHEDULED:
                                job_node.status = JobStatus.UNPREDICTED_PREFETCHING

                        if job.status == JobStatus.PREFETCHING:
                            prefetching_queue.put(job)
                            scheduled_queue.get()
                        elif job.status == JobStatus.UNPREDICTED_PREFETCHING:
                            unpredicted_prefetching_queue.put(job)
                            unpredicted_scheduled_queue.get()

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
            unpredicted_prefetching_queue = self.job_statuses[JobStatus.UNPREDICTED_PREFETCHING]
            unpredicted_prefetched_queue = self.job_statuses[JobStatus.UNPREDICTED_PREFETCHED]
            prefetched_queue = self.job_statuses[JobStatus.PREFETCHED]
            failed_queue = self.job_statuses[JobStatus.FAILED]

            for _ in range(prefetching_queue.qsize() + unpredicted_prefetching_queue.qsize()):
                self.thread_statuses["prefetcher_poll_thread"] = True

                if prefetching_queue.empty():
                    job = unpredicted_prefetched_queue.get()
                else:
                    job = prefetching_queue.get()

                logger.error(f"{job.file_path} POLL PREFETCH")
                file_path = job.file_path
                worker_id = job.worker_id
                prefetcher = self.prefetchers[worker_id]

                prefetcher_status = prefetcher.get_transfer_status(file_path)
                if prefetcher_status == PrefetcherStatuses.SUCCEEDED:
                    for job_node in job.bfs_iterator(include_root=True):
                        if job_node.status == JobStatus.PREFETCHING:
                            job_node.status = JobStatus.PREFETCHED
                        elif job_node.status == JobStatus.UNPREDICTED_PREFETCHING:
                            job_node.status = JobStatus.UNPREDICTED_PREFETCHED

                    if job.status == JobStatus.PREFETCHED:
                        prefetched_queue.put(job)
                    elif job.status == JobStatus.UNPREDICTED_PREFETCHED:
                        unpredicted_prefetched_queue.put(job)
                elif prefetcher_status == PrefetcherStatuses.FAILED:
                    for job_node in job.bfs_iterator(include_root=True):
                        if job_node.status == JobStatus.PREFETCHING or job_node.status == JobStatus.UNPREDICTED_PREFETCHING:
                            job_node.status = JobStatus.FAILED
                    logger.error(f"{job.file_path} FAILED TO PREFETCH")
                    # Potentially add more logic here or in prefetcher to restart failed transfer
                    failed_queue.put(job)
                else:
                    if job.status == JobStatus.PREFETCHING:
                        prefetching_queue.put(job)
                    elif job.status == JobStatus.UNPREDICTED_PREFETCHING:
                        unpredicted_prefetching_queue.put(job)

            self.thread_statuses["prefetcher_poll_thread"] = False
            time.sleep(5)

    def _thread_funcx_process_headers(self) -> None:
        """Thread function to submit header processing tasks to funcX.

        Returns
        -------
        None
        """
        while not self.kill_status:
            unpredicted_prefetched_queue = self.job_statuses[JobStatus.UNPREDICTED_PREFETCHED]
            processing_headers_queue = self.job_statuses[JobStatus.PROCESSING_HEADERS]

            while not unpredicted_prefetched_queue.empty():
                self.thread_statuses["funcx_processing_headers_thread"] = True
                job = unpredicted_prefetched_queue.get()
                logger.error(f"{job.file_path} PROCESSING HEADERS")
                job_dict = Job.to_dict(job)
                worker_id = job.worker_id

                worker = self.worker_dict[worker_id]
                funcx_task_id = self.funcx_client.run(job_dict,
                                                      worker.transfer_dir,
                                                      endpoint_id=worker.funcx_eid,
                                                      function_id=PROCESS_HEADER_FUNCX_UUID)

                job.funcx_process_headers_id = funcx_task_id
                job.status = JobStatus.PROCESSING_HEADERS

                processing_headers_queue.put(job)

                time.sleep(1)

            self.thread_statuses["funcx_processing_headers_thread"] = False

    # TODO: Consolidate this and _thread_funcx_crawl into one function
    def _thread_funcx_decompress(self) -> None:
        """Thread function to submit decompression tasks to funcX.

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
                logger.error(f"{job.file_path} DECOMPRESSING")
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
        """Thread function to submit crawl tasks to funcX.
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
                logger.error(f"{job.file_path} CRAWLING")
                job_dict = Job.to_dict(job)
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
        unpredicted_queue = self.job_statuses[JobStatus.UNPREDICTED]
        decompressing_queue = self.job_statuses[JobStatus.DECOMPRESSING]
        decompressed_queue = self.job_statuses[JobStatus.DECOMPRESSED]
        crawling_queue = self.job_statuses[JobStatus.CRAWLING]
        processing_headers_queue = self.job_statuses[JobStatus.PROCESSING_HEADERS]
        predicted_queue = self.job_statuses[JobStatus.PREDICTED]
        consolidating_queue = self.job_statuses[JobStatus.CONSOLIDATING]
        failed_queue = self.job_statuses[JobStatus.FAILED]

        while not self.kill_status:
            for _ in range(processing_headers_queue.qsize()):
                self.thread_statuses["funcx_poll_thread"] = True
                job = processing_headers_queue.get()
                logger.error(f"{job.file_path} POLLING HEADER PROCESSING")
                funcx_process_headers_id = job.funcx_process_headers_id
                worker = self.worker_dict[job.worker_id]
                try:
                    result = self.funcx_client.get_result(funcx_process_headers_id)
                    job = Job.from_dict(result)
                    logger.error(f"{job.file_path} COMPLETED HEADER PROCESSING")

                    job.status = JobStatus.PREDICTED

                    worker.curr_available_space += job.compressed_size
                    consolidating_queue.put(job)
                except Exception as e:
                    if is_non_critical_funcx_error(e):
                        processing_headers_queue.put(job)
                    else:
                        failed_queue.put(job)

                time.sleep(5)

            for _ in range(decompressing_queue.qsize()):
                self.thread_statuses["funcx_poll_thread"] = True
                job = decompressing_queue.get()
                logger.error(f"{job.file_path} POLLING DECOMPRESS")
                funcx_decompress_id = job.funcx_decompress_id
                worker = self.worker_dict[job.worker_id]

                try:
                    result = self.funcx_client.get_result(funcx_decompress_id)
                    job = Job.from_dict(result)
                    logger.error(f"{job.file_path} COMPLETED DECOMPRESS")

                    if job.status == JobStatus.FAILED:
                        failed_queue.put(job)
                        continue
                    if job.status == JobStatus.UNPREDICTED:
                        unpredicted_queue.put(job)
                        continue

                    for job_node in job.bfs_iterator(include_root=True):
                        if job_node.status == JobStatus.DECOMPRESSING:
                            job_node.status = JobStatus.DECOMPRESSED

                    worker.curr_available_space += job.compressed_size

                    decompressed_queue.put(job)
                except Exception as e:
                    logger.error(f"ERROR for {job.file_path}: {e}")
                    if is_non_critical_funcx_error(e):
                        decompressing_queue.put(job)
                    else:
                        failed_queue.put(job)

                time.sleep(5)

            for _ in range(crawling_queue.qsize()):
                self.thread_statuses["funcx_poll_thread"] = True
                job = crawling_queue.get()
                logger.error(f"{job.file_path} POLLING CRAWL")
                funcx_crawl_id = job.funcx_crawl_id
                worker = self.worker_dict[job.worker_id]
                try:
                    result = self.funcx_client.get_result(funcx_crawl_id)
                    job = Job.from_dict(result)
                    logger.error(f"{job.file_path} COMPLETED CRAWL")

                    for job_node in job.bfs_iterator(include_root=True):
                        if job_node.status == JobStatus.CRAWLING:
                            job_node.status = JobStatus.CONSOLIDATING

                    #TODO: Check if this is correct
                    worker.curr_available_space += (job.total_size - job.compressed_size)
                    consolidating_queue.put(job)
                except Exception as e:
                    if is_non_critical_funcx_error(e):
                        crawling_queue.put(job)
                    else:
                        failed_queue.put(job)

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
            failed_queue = self.job_statuses[JobStatus.FAILED]

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
                            if "decompressed_size" in file_metadata["physical"]:
                                decompressed_size = file_metadata["physical"]["decompressed_size"]
                            if child_file_path in job_node.child_jobs:
                                break
                            else:
                                child_job = Job(
                                    file_path=child_file_path,
                                    file_id=f"{str(uuid.uuid4())}",
                                    compressed_size=file_size
                                )

                                if decompressed_size:
                                    child_job.decompressed_size = decompressed_size
                                    child_job.status = JobStatus.PREDICTED
                                else:
                                    child_job.status = JobStatus.UNPREDICTED

                                job_node.child_jobs[child_file_path] = child_job
                                resubmit_task = True

                if resubmit_task:
                    logger.error(f"{job.file_path} RESUBMITTING")
                    unpredicted_queue.put(job)
                    continue

                consolidated_metadata = job.consolidate_metadata()
                self.abyss_metadata.append(consolidated_metadata)

                for job_node in job.bfs_iterator(include_root=True):
                    if job_node.status == JobStatus.CONSOLIDATING:
                        job_node.status = JobStatus.SUCCEEDED

                succeeded_queue.put(job)

            while not failed_queue.empty():
                job = failed_queue.get()
                consolidated_metadata = job.consolidate_metadata()
                self.abyss_metadata.append(consolidated_metadata)
                succeeded_queue.put(job)

            self.thread_statuses["consolidate_results_thread"] = False


if __name__ == "__main__":
    import pandas as pd
    import os
    from abyss.utils.psql_utils import read_db_config_file, create_connection
    from abyss.utils.aws_utils import create_s3_connection, read_aws_config_file

    PROJECT_ROOT = os.path.realpath(os.path.dirname(__file__)) + "/"
    logger.error(PROJECT_ROOT)
    deep_blue_crawl_df = pd.read_csv("/Users/ryan/Documents/CS/abyss/data/deep_blue_crawl.csv")
    filtered_files = deep_blue_crawl_df[deep_blue_crawl_df.extension == "zip"].sort_values(by=["size_bytes"]).iloc[0:100]

    logger.error(sum(filtered_files.size_bytes))


    workers = [{"globus_eid": "f4e2f5a4-a186-11eb-8a91-d70d98a40c8d",
                "funcx_eid": "66dab10e-d323-41e1-8f4a-4bfc3204357e",
                "max_available_space": 3*10**9,
                "transfer_dir": "/home/tskluzac/ryan/deep_blue_data",
                "decompress_dir": "/home/tskluzac/ryan/results"}]

    compressed_files = [{"file_path": x[0], "compressed_size": x[1]} for _, x in filtered_files.iterrows()]
    # compressed_files = [{"file_path": "/UMich/download/DeepBlueData_gt54kn05f/NAmerica_current_30arcsec_generic_set1.zip", "compressed_size": 290392819}]
    transfer_token = 'AgQWYYnwxWNGnlwVvdXJjwWY9xMkaN4Kg9VnX7GWbzNd09kEwWUvC3MYdW591GJObmNMWz6N980j41FMPa5zWTE3MK'
    abyss_id = str(uuid.uuid4())
    logger.error(abyss_id)

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
    logger.error(time.time() - t0)
