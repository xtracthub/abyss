import logging
import threading
import time
import uuid
from enum import Enum
from queue import Queue
from typing import List, Union

from abyss.orchestrator.job import Job

import globus_sdk

logger = logging.getLogger(__name__)


class PrefetcherStatuses(Enum):
    QUEUED = "QUEUED"
    ACTIVE = "ACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


#TODO: Does not properly handle failed transfers. If a transfer fails at
# any point, the thread will terminate regardless of if the transfer is
# fatal.
class GlobusPrefetcher:
    def __init__(self, transfer_token: str, globus_source_eid: str,
                 globus_dest_eid: str, transfer_dir: str,
                 max_concurrent_transfers=4, max_files_per_batch=100,
                 max_batch_size=10*10**9):
        """Class for facilitating transfers to a Globus endpoint.

        Parameters
        ----------
        transfer_token : str
            Authorization token to transfer Globus files.
        globus_source_eid : str
            Globus endpoint ID of source data storage.
        globus_dest_eid : str
            Globus destination ID of source data storage.
        transfer_dir : str
            Directory at endpoint to transfer files to.
        """
        self.transfer_token = transfer_token
        self.globus_source_eid = globus_source_eid
        self.globus_dest_eid = globus_dest_eid
        self.transfer_dir = transfer_dir
        self.max_concurrent_transfers = max_concurrent_transfers
        self.max_files_per_batch = max_files_per_batch
        self.max_batch_size = max_batch_size

        self.files_to_transfer = Queue()
        self.num_current_transfers = 0
        self.file_id_mapping = dict()
        self.id_status = dict()
        self.lock = threading.Lock()

        self._get_transfer_client()

    def transfer_job(self, job: Job) -> None:
        """Submits a file path to queue for transferring.

        Parameters
        ----------
        job: Job
            Job object of file to transfer.

        Returns
        -------
        None
        """
        self.files_to_transfer.put(job)

        with self.lock:
            # Dummy UUID until file is actually submitted
            file_id = uuid.uuid4()
            self.file_id_mapping[job.file_path] = file_id
            self.id_status[file_id] = PrefetcherStatuses.QUEUED

            logger.info(f"{job.file_path}: QUEUED")

        self._start_thread()

    def transfer_job_batch(self, jobs: List[Job]) -> None:
        """Submits a list of file_paths to queue for transferring.

        Parameters
        ----------
        jobs : list(Job)
            List of jobs to transfer as a batch.

        Returns
        -------
        None
        """
        with self.lock:
            idx = 0
            while idx < len(jobs):
                job_batch = []
                job_batch_size = 0
                job_batch_num_files = 0

                while job_batch_size <= self.max_batch_size and job_batch_num_files < self.max_files_per_batch and idx < len(jobs):
                    job = jobs[idx]
                    job_batch.append(job)

                    file_path = job.file_path
                    # Dummy UUID until file is actually submitted
                    file_id = uuid.uuid4()
                    self.file_id_mapping[file_path] = file_id
                    self.id_status[file_id] = PrefetcherStatuses.QUEUED

                    idx += 1
                    job_batch_size += job.compressed_size
                    job_batch_num_files += 1

                    logger.info(f"{file_path}: QUEUED")

                self.files_to_transfer.put(job_batch)

        self._start_thread()

    def get_transfer_status(self, file_path: str) -> Union[str, None]:
        """Retrieves the transfer status of a file path.

        Parameters
        ----------
        file_path : str
            File path to get status of.

        Returns
        -------
        status : str
            Transfer status of file path. Either "QUEUED", "ACTIVE",
            "SUCCEEDED", or "FAILED".
        """
        try:
            status = self.id_status[self.file_id_mapping[file_path]]
            return status
        except KeyError:
            print(file_path)
            print(self.id_status)
            print(self.file_id_mapping)
            return None

    def _start_thread(self) -> None:
        """Spins up threads to process jobs from queue.

        Returns
        -------
        None
        """
        while self.num_current_transfers < self.files_to_transfer.qsize():
            if self.num_current_transfers >= self.max_concurrent_transfers:
                break
            else:
                threading.Thread(target=self._thread_transfer).start()

                with self.lock:
                    self.num_current_transfers += 1

    def _thread_transfer(self) -> None:
        """Thread function for submitting transfer tasks and waiting for
        results.

        Returns
        -------
        None
        """
        while not(self.files_to_transfer.empty()):
            task = self.files_to_transfer.get()

            if isinstance(task, list):
                self._thread_transfer_job_batch(task)
            else:
                self._thread_transfer_job(task)

    def _thread_transfer_job(self, job: Job) -> None:
        """Thread function for transferring individual files.

        Parameters
        ----------
        job : Job
            Job object of file to transfer.

        Returns
        -------
        None
        """
        try:
            tdata = globus_sdk.TransferData(self.tc,
                                            self.globus_source_eid,
                                            self.globus_dest_eid)
        except globus_sdk.exc.TransferAPIError as e:
            logger.error(f"Prefetcher caught {e}")

            with self.lock:
                source_file_path = job.file_path
                task_id = self.file_id_mapping[source_file_path]

                self.id_status[task_id] = PrefetcherStatuses.FAILED

            return

        source_file_path = job.file_path
        dest_file_name = job.file_id

        full_path = f"{self.transfer_dir}/{dest_file_name}"
        tdata.add_item(source_file_path,
                       full_path)

        task_id = self.tc.submit_transfer(tdata)["task_id"]

        with self.lock:
            self.file_id_mapping[source_file_path] = task_id
            self.id_status[task_id] = PrefetcherStatuses.ACTIVE

            logger.info(f"{source_file_path}: ACTIVE")

        task_data = self.tc.get_task(task_id).data
        while task_data["status"] == "ACTIVE":
            time.sleep(5)
            task_data = self.tc.get_task(task_id).data

        with self.lock:
            self.id_status[task_id] = PrefetcherStatuses[task_data["status"]]
            self.num_current_transfers -= 1

            logger.info(f"{source_file_path}: {task_data['status']}")

    def _thread_transfer_job_batch(self, jobs: List[Job]) -> None:
        """Thread function for transferring list of files.

        Parameters
        ----------
        jobs : list(Job)
            List of Job objects to transfer as a batch.

        Returns
        -------
        None
        """
        try:
            tdata = globus_sdk.TransferData(self.tc,
                                            self.globus_source_eid,
                                            self.globus_dest_eid)
        except globus_sdk.exc.TransferAPIError as e:
            logger.error(f"Prefetcher caught {e}")

            with self.lock:
                for job in jobs:
                    source_file_path = job.file_path
                    task_id = self.file_id_mapping[source_file_path]

                    self.id_status[task_id] = PrefetcherStatuses.FAILED

            return

        for job in jobs:
            source_file_path = job.file_path
            dest_file_name = job.file_id

            full_path = f"{self.transfer_dir}/{dest_file_name}"
            tdata.add_item(source_file_path,
                           full_path)

        task_id = self.tc.submit_transfer(tdata)["task_id"]

        with self.lock:
            for job in jobs:
                file_path = job.file_path
                self.file_id_mapping[file_path] = task_id
            self.id_status[task_id] = PrefetcherStatuses.ACTIVE

            for job in jobs:
                file_path = job.file_path
                logger.info(f"{file_path}: ACTIVE")

        task_data = self.tc.get_task(task_id).data
        while task_data["status"] == "ACTIVE":
            time.sleep(5)
            task_data = self.tc.get_task(task_id).data

        with self.lock:
            self.id_status[task_id] = PrefetcherStatuses[task_data["status"]]
            self.num_current_transfers -= 1

            for job in jobs:
                file_path = job.file_path
                logger.info(f"{file_path}: {task_data['status']}")

    def _get_transfer_client(self) -> None:
        """Sets self.tc to Globus transfer client using
        self.transfer_token as authorization.

        Returns
        -------
        None
        """
        authorizer = globus_sdk.AccessTokenAuthorizer(self.transfer_token)

        self.tc = globus_sdk.TransferClient(authorizer=authorizer)


if __name__ == "__main__":
    prefetcher = GlobusPrefetcher("AgEqE5QBmdy5NBEyqM1Gx2N4mN299MWN0Y2pPjOvNxqGjMEBpyiwCegxa3MnylpyjDYoQ1bXKjmVYyTygwbYkcp5gz",
                                  "4f99675c-ac1f-11ea-bee8-0e716405a293",
                                  "af7bda53-6d04-11e5-ba46-22000b92c6ec",
                                  "/project2/chard/skluzacek/ryan-data/transfer_dir",
                                  max_concurrent_transfers=4,
                                  max_files_per_batch=10,
                                  max_batch_size=1*10**9)

    import pandas as pd

    deep_blue_crawl_df = pd.read_csv("/Users/ryan/Documents/CS/abyss/data/deep_blue_crawl.csv")

    sorted_files = deep_blue_crawl_df.sort_values(by=["size_bytes"])

    filtered_files = sorted_files.iloc[0:10]

    compressed_files = [{"file_path": x[0], "compressed_size": x[1]} for _, x in filtered_files.iterrows()]

    for compressed_file in compressed_files:
        job = Job.from_dict(compressed_file)
        job.file_id = str(uuid.uuid4())
        prefetcher.transfer_job(job)




