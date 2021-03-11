import os
import threading
import time
import uuid
from queue import Queue

import globus_sdk

from abyss.crawlers.crawler import Crawler
from abyss.decompressors import is_compressed
from abyss.crawlers.groupers import get_grouper


GLOBUS_CRAWLER_FUNCX_UUID = "e0476019-8b77-4e96-845b-4dabd443e699"


class GlobusCrawler(Crawler):
    def __init__(self, transfer_token: str,
                 globus_eid: str, base_path: str, grouper_name: str,
                 max_crawl_threads=2):
        """Crawls and groups files within a Globus directory, then pushes
        results to an SQS queue. Crawler status is recorded in a PostgreSQL
        database.

        Parameters
        ----------
        transfer_token : str
            Authorization for accessing Globus endpoint.
        globus_eid : str
            Globus endpoint to crawl.
        base_path : str
            Location in endpoint to begin crawling.
        grouper_name : str
            Name of groupers to use.
        max_crawl_threads : int
            Max number of threads to use to crawl.
        """
        self.transfer_token = transfer_token
        self.globus_eid = globus_eid
        self.base_path = base_path
        self.max_crawl_threads = max_crawl_threads

        self.crawl_id = str(uuid.uuid4())
        self.crawl_results = {"root_path": base_path, "metadata": []}
        self.crawl_queue = Queue()
        self.crawl_threads_status = dict()
        self.grouper = get_grouper(grouper_name)

        self._get_transfer_client()

    def crawl(self) -> dict:
        """Method for starting local crawl.

        Returns
        -------
        self.crawl_results: dict
            Dictionary containing crawl metadata
        """
        self._start_crawl()

        return self.crawl_results

    def _start_crawl(self) -> None:
        """Internal blocking method for starting local crawl. Starts all
        threads and updates database with crawl status."""
        self.crawl_queue.put(self.base_path)

        crawl_threads = []
        for i in range(self.max_crawl_threads):
            thread_id = str(uuid.uuid4())
            thread = threading.Thread(target=self._thread_crawl,
                                      args=(thread_id,),
                                      daemon=True)
            thread.start()
            crawl_threads.append(thread)
            self.crawl_threads_status[thread_id] = "WORKING"

        for thread in crawl_threads:
            thread.join()

    def _thread_crawl(self, thread_id: str) -> None:
        """Crawling thread."""
        while True:
            while self.crawl_queue.empty():
                if all([status in ("IDLE", "FINISHED") for status in
                        self.crawl_threads_status.values()]):
                    self.crawl_threads_status[thread_id] = "FINISHED"
                    return
                else:
                    self.crawl_threads_status[thread_id] = "IDLE"
                    time.sleep(1)

            self.crawl_threads_status[thread_id] = "WORKING"

            curr = self.crawl_queue.get()
            dir_file_metadata = {}

            try:
                for item in self.tc.operation_ls(self.globus_eid, path=curr):
                    item_name = item["name"]
                    full_path = os.path.join(curr, item_name)

                    if item["type"] == "file":
                        extension = self.get_extension(full_path)
                        file_size = item["size"]

                        dir_file_metadata[full_path] = {
                            "physical": {
                                "size": file_size,
                                "extension": extension,
                                "is_compressed": is_compressed(full_path)
                            }
                        }
                    elif item["type"] == "dir":
                        self.crawl_queue.put(full_path)
            except globus_sdk.exc.TransferAPIError as e:
                if e.code == "ExternalError.DirListingFailed.NotDirectory":
                    for item in self.tc.operation_ls(self.globus_eid,
                                                     path=os.path.dirname(curr)):
                        full_path = os.path.join(os.path.dirname(curr),
                                                 item["name"])
                        if full_path == curr:
                            extension = self.get_extension(curr)
                            file_size = item["size"]

                            file_metadata = {
                                "physical": {
                                    "size": file_size,
                                    "extension": extension
                                }
                            }
                            self.crawl_results["metadata"].append({"path": curr,
                                                                   "metadata": file_metadata})
                            break

            for path, metadata in dir_file_metadata.items():
                self.crawl_results["metadata"].append({"path": path,
                                                       "metadata": dir_file_metadata[path]})

    def _get_transfer_client(self) -> None:
        """Sets self.tc to Globus transfer client using
        self.transfer_token as authorization.

        Returns
        -------
        None
        """
        authorizer = globus_sdk.AccessTokenAuthorizer(
            self.transfer_token)

        self.tc = globus_sdk.TransferClient(authorizer=authorizer)
