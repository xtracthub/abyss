import os
import threading
import time
import uuid
from queue import Queue
import globus_sdk
from xtract_sdk.packagers import Family
from abyss.crawler.crawler import Crawler
from abyss.grouper import get_grouper
from abyss.utils.sqs_utils import put_message, make_queue
from abyss.utils.psql_utils import create_table_entry, update_table_entry, \
    select_by_column


class GlobusCrawler(Crawler):
    def __init__(self, abyss_id: str, transfer_token: str,
                 globus_eid: str, base_path: str, grouper_name: str,
                 conn, aws_access: str, aws_secret: str,
                 region_name: str, max_crawl_threads=2, max_push_threads=4):
        """Crawls and groups files within a Globus directory, then pushes
        results to an SQS queue. Crawler status is recorded in a PostgreSQL
        database.

        Parameters
        ----------
        abyss_id : str
            ID of Abyss job running the crawl task.
        transfer_token : str
            Authorization for accessing Globus endpoint.
        globus_eid : str
            Globus endpoint to crawl.
        base_path : str
            Location in endpoint to begin crawling.
        grouper_name : str
            Name of grouper to use.
        conn
            Connection object to PostgreSQL database.
        aws_access : str
            AWS access key for accessing SQS queue.
        aws_secret : str
            AWS secret key for accessing SQS queue.
        region_name : str
            AWS region name for SQS.
        max_crawl_threads : int
            Max number of threads to use to crawl.
        max_push_threads : str
            Max number of threads to use to push results to SQS.
        """
        self.abyss_id = abyss_id
        self.transfer_token = transfer_token
        self.globus_eid = globus_eid
        self.base_path = base_path
        self.max_crawl_threads = max_crawl_threads
        self.max_push_threads = max_push_threads
        self.db_conn = conn
        self.aws_access = aws_access
        self.aws_secret = aws_secret
        self.region_name = region_name

        self.crawl_id = str(uuid.uuid4())
        self.sqs_queue_name = f"crawl_{self.crawl_id}"
        self.crawl_queue = Queue()
        self.push_queue = Queue()
        self.crawl_threads_status = dict()
        self.push_threads_status = dict()
        self.grouper = get_grouper(grouper_name)

        self._get_transfer_client()
        make_queue(self.sqs_queue_name, self.aws_access,
                   self.aws_secret,
                   self.region_name)

    def crawl(self):
        """Non-blocking method for starting local crawl.

        Returns
        -------
        str
            Crawl ID.
        """
        threading.Thread(target=self._start_crawl).start()

        return self.crawl_id

    def get_status(self) -> str:
        """Pulls crawl status from database.

        Returns
        -------
        str
            Crawl status for crawl.
        """
        crawl_status_entry = select_by_column(self.db_conn, "crawl_status",
                                              **{"crawl_id": self.crawl_id})

        return crawl_status_entry[0]["crawl_id"]

    def _start_crawl(self):
        """Internal blocking method for starting local crawl. Starts all
        threads and updates database with crawl status."""
        self.crawl_queue.put(self.base_path)

        create_table_entry(self.db_conn, "crawl_status",
                           **{"crawl_id": self.crawl_id,
                              "crawl_status": "STARTING"})

        crawl_threads = []
        for i in range(self.max_crawl_threads):
            thread_id = str(uuid.uuid4())
            thread = threading.Thread(target=self._thread_crawl,
                                      args=(thread_id,))
            thread.start()
            crawl_threads.append(thread)
            self.crawl_threads_status[thread_id] = "WORKING"

        update_table_entry(self.db_conn, "crawl_status",
                           {"crawl_id": self.crawl_id},
                           **{"crawl_status": "CRAWLING"})

        push_threads = []
        for _ in range(self.max_push_threads):
            thread_id = str(uuid.uuid4())
            thread = threading.Thread(target=self._thread_push,
                                      args=(thread_id,))
            thread.start()
            push_threads.append(thread)
            self.push_threads_status[thread_id] = "WORKING"

        for thread in crawl_threads:
            thread.join()

        update_table_entry(self.db_conn, "crawl_status",
                           {"crawl_id": self.crawl_id},
                           **{"crawl_status": "PUSHING"})

        for thread in push_threads:
            thread.join()

        update_table_entry(self.db_conn, "crawl_status",
                           {"crawl_id": self.crawl_id},
                           **{"crawl_status": "COMPLETE"})

    def _thread_crawl(self, thread_id):
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

            for item in self.tc.operation_ls(self.globus_eid, path=curr):
                item_name = item["name"]
                full_path = os.path.join(curr, item_name)

                if item["type"] == "file":
                    extension = self.get_extension(full_path)
                    file_size = item["size"]

                    dir_file_metadata[full_path] = {
                        "physical": {
                            "size": file_size,
                            "extension": extension
                        }
                    }
                elif item["type"] == "dir":
                    self.crawl_queue.put(full_path)

            for path, metadata in dir_file_metadata.items():
                self.push_queue.put({"path": path,
                                     "metadata": dir_file_metadata[path]})

    def _thread_push(self, thread_id):
        """SQS pushing thread."""
        while True:
            while self.push_queue.empty():
                if all([status in ("IDLE", "FINISHED") for status in self.crawl_threads_status.values()]):
                    if all([status in ("IDLE", "FINISHED") for status in self.push_threads_status.values()]):
                        self.push_threads_status[thread_id] = "FINISHED"
                        return
                    else:
                        self.push_threads_status[thread_id] = "IDLE"
                        time.sleep(1)
                else:
                    self.push_threads_status[thread_id] = "IDLE"
                    time.sleep(1)

            self.push_threads_status[thread_id] = "WORKING"
            message = self.push_queue.get()

            put_message(message, self.sqs_queue_name,
                        self.aws_access, self.aws_secret,
                        self.region_name)

    def _get_transfer_client(self):
        """Sets self.tc to Globus transfer client using
        self.transfer_token as authorization.

        Returns
        -------
        None
        """
        authorizer = globus_sdk.AccessTokenAuthorizer(
            self.transfer_token)

        self.tc = globus_sdk.TransferClient(authorizer=authorizer)
