import logging
import threading
import time
import uuid
from enum import Enum
from queue import Queue
from typing import Dict, List, Union

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
                 max_concurrent_transfers: int):
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

        self.files_to_transfer = Queue()
        self.num_current_transfers = 0
        self.file_id_mapping = dict()
        self.id_status = dict()
        self.lock = threading.Lock()

        self._get_transfer_client()

    def transfer(self, source_file_path: str, dest_file_name: str) -> None:
        """Submits a file path to queue for transferring.

        Parameters
        ----------
        source_file_path : str
            File path of file on globus_source_eid to transfer.
        dest_file_name : str
            Name of give to transferred file on globus_source_eid.

        Returns
        -------
        None
        """
        self.files_to_transfer.put({
            "source_file_path": source_file_path,
            "dest_file_name": dest_file_name
        })

        with self.lock:
            # Dummy UUID until file is actually submitted
            file_id = uuid.uuid4()
            self.file_id_mapping[source_file_path] = file_id
            self.id_status[file_id] = PrefetcherStatuses.QUEUED

            logger.info(f"{source_file_path}: QUEUED")

        self._start_thread()

    def transfer_batch(self, file_path_mappings: List[Dict]) -> None:
        """Submits a list of file_paths to queue for transferring.

        Parameters
        ----------
        file_path_mappings : list(dict)
            List of dictionaries mapping "source_file_path" to file path
            of file on globus_source_eid to transfer and "dest_file_name"
            to name of give to transferred file on globus_source_eid.

        Returns
        -------
        None
        """
        for file_path_mapping in file_path_mappings:
            assert "source_file_path" in file_path_mapping, "Transfer batch item missing source_file_path"
            assert "dest_file_name" in file_path_mapping, "Transfer batch item missing dest_file_name"

        self.files_to_transfer.put(file_path_mappings)

        with self.lock:
            for file_path_mapping in file_path_mappings:
                file_path = file_path_mapping["source_file_path"]
                # Dummy UUID until file is actually submitted
                file_id = uuid.uuid4()
                self.file_id_mapping[file_path] = file_id
                self.id_status[file_id] = PrefetcherStatuses.QUEUED

                logger.info(f"{file_path}: QUEUED")

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
                self._thread_transfer_batch(task)
            else:
                self._thread_transfer_file(task)

    def _thread_transfer_file(self, file_path_mapping: dict) -> None:
        """Thread function for transferring individual files.

        Parameters
        ----------
        file_path_mapping : dict
            Dictionary mapping "source_file_path" to file path
            of file on globus_source_eid to transfer and "dest_file_name"
            to name of give to transferred file on globus_source_eid.

        Returns
        -------
        None
        """
        tdata = globus_sdk.TransferData(self.tc,
                                        self.globus_source_eid,
                                        self.globus_dest_eid)

        source_file_path = file_path_mapping["source_file_path"]
        dest_file_name = file_path_mapping["dest_file_name"]

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

    def _thread_transfer_batch(self, file_path_mappings: List[Dict]) -> None:
        """Thread function for transferring list of files.

        Parameters
        ----------
        file_path_mappings : list(dict)
            List of dictionaries mapping "source_file_path" to file path
            of file on globus_source_eid to transfer and "dest_file_name"
            to name of give to transferred file on globus_source_eid.

        Returns
        -------
        None
        """
        tdata = globus_sdk.TransferData(self.tc,
                                        self.globus_source_eid,
                                        self.globus_dest_eid)

        for file_path_mapping in file_path_mappings:
            source_file_path = file_path_mapping["source_file_path"]
            dest_file_name = file_path_mapping["dest_file_name"]

            full_path = f"{self.transfer_dir}/{dest_file_name}"
            tdata.add_item(source_file_path,
                           full_path)

        task_id = self.tc.submit_transfer(tdata)["task_id"]

        with self.lock:
            for file_path_mapping in file_path_mappings:
                file_path = file_path_mapping["source_file_path"]
                self.file_id_mapping[file_path] = task_id
            self.id_status[task_id] = PrefetcherStatuses.ACTIVE

            for file_path_mapping in file_path_mappings:
                file_path = file_path_mapping["source_file_path"]
                logger.info(f"{file_path}: ACTIVE")

        task_data = self.tc.get_task(task_id).data
        while task_data["status"] == "ACTIVE":
            time.sleep(5)
            task_data = self.tc.get_task(task_id).data

        with self.lock:
            self.id_status[task_id] = PrefetcherStatuses[task_data["status"]]
            self.num_current_transfers -= 1

            for file_path_mapping in file_path_mappings:
                file_path = file_path_mapping["source_file_path"]
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
