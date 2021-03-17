import os
import threading
import time
import uuid
from queue import Queue
from abyss.crawlers.crawler import Crawler
from abyss.crawlers.groupers import get_grouper
from abyss.utils.aws_utils import put_messages, make_queue


@DeprecationWarning
class LocalCrawler(Crawler):
    def __init__(self, abyss_id: str, base_path: str, grouper_name: str,
                 conn, sqs_conn, max_crawl_threads=2, max_push_threads=4):
        """Crawls and groups files within a local directory, then pushes
        results to an SQS queue. Crawler status is recorded in a PostgreSQL
        database.

        Parameters
        ----------
        abyss_id : str
            ID of Abyss job running the crawl task.
        base_path : str
            Location in directory to begin crawling.
        grouper_name : str
            Name of groupers to use.
        conn
            Connection object to PostgreSQL database.
        sqs_conn
            AWS SQS boto3 object.
        max_crawl_threads : int
            Max number of threads to use to crawl.
        max_push_threads : str
            Max number of threads to use to push results to SQS.
        """
        self.abyss_id = abyss_id
        self.base_path = base_path
        self.max_crawl_threads = max_crawl_threads
        self.max_push_threads = max_push_threads
        self.db_conn = conn
        self.sqs_conn = sqs_conn

        self.crawl_id = str(uuid.uuid4())
        self.sqs_queue_name = f"crawl_{self.crawl_id}"
        self.crawl_queue = Queue()
        self.push_queue = Queue()
        self.crawl_threads_status = dict()
        self.push_threads_status = dict()
        self.grouper = get_grouper(grouper_name)
        self.crawl_status = "STARTING"

        make_queue(self.sqs_conn, self.sqs_queue_name)

    def crawl(self, blocking=True):
        """Method for starting local crawl.

        Parameters
        -------
        blocking : bool
            Whether crawl method should be blocking.

        Returns
        -------
        str
            Crawl ID.
        """
        crawl_thread = threading.Thread(target=self._start_crawl)
        crawl_thread.start()

        if blocking:
            crawl_thread.join()

        return self.crawl_id

    def get_status(self) -> str:
        """Pulls crawl status from database.

        Returns
        -------
        str
            Crawl status for crawl.
        """
        # crawl_status_entry = select_by_column(self.db_conn, "crawl_status",
        #                                       **{"crawl_id": self.crawl_id})

        # return crawl_status_entry[0]["crawl_status"]
        return self.crawl_status

    def _start_crawl(self):
        """Internal blocking method for starting local crawl. Starts all
        threads and updates database with crawl status."""
        self.crawl_queue.put(self.base_path)

        # create_table_entry(self.db_conn, "crawl_status",
        #                    **{"crawl_id": self.crawl_id,
        #                       "crawl_status": "STARTING"})

        crawl_threads = []
        for i in range(self.max_crawl_threads):
            thread_id = str(uuid.uuid4())
            thread = threading.Thread(target=self._thread_crawl,
                                      args=(thread_id,))
            thread.start()
            crawl_threads.append(thread)
            self.crawl_threads_status[thread_id] = "WORKING"

        # update_table_entry(self.db_conn, "crawl_status",
        #                    {"crawl_id": self.crawl_id},
        #                    **{"crawl_status": "CRAWLING"})

        self.crawl_status = "CRAWLING"

        for thread in crawl_threads:
            thread.join()

        push_threads = []
        for _ in range(self.max_push_threads):
            thread_id = str(uuid.uuid4())
            thread = threading.Thread(target=self._thread_push,
                                      args=(thread_id,))
            thread.start()
            push_threads.append(thread)
            self.push_threads_status[thread_id] = "WORKING"

        # update_table_entry(self.db_conn, "crawl_status",
        #                    {"crawl_id": self.crawl_id},
        #                    **{"crawl_status": "PUSHING"})

        self.crawl_status = "PUSHING"

        for thread in push_threads:
            thread.join()

        # update_table_entry(self.db_conn, "crawl_status",
        #                    {"crawl_id": self.crawl_id},
        #                    **{"crawl_status": "COMPLETE"})

        self.crawl_status = "SUCCEEDED"

    def _thread_crawl(self, thread_id):
        """Crawling thread."""
        while True:
            while self.crawl_queue.empty():
                if all([status in ("IDLE", "FINISHED") for status in self.crawl_threads_status.values()]):
                    self.crawl_threads_status[thread_id] = "FINISHED"
                    return
                else:
                    self.crawl_threads_status[thread_id] = "IDLE"
                    time.sleep(1)

            self.crawl_threads_status[thread_id] = "WORKING"

            curr = self.crawl_queue.get()
            dir_file_metadata = {}
            file_ls = [os.path.join(curr, file) for file in os.listdir(curr)]

            for path in file_ls:
                if os.path.isfile(path):
                    extension = self.get_extension(path)
                    file_size = os.path.getsize(path)

                    dir_file_metadata[path] = {
                        "physical": {
                            "size": file_size,
                            "extension": extension
                        }
                    }

                    self.push_queue.put({"path": path,
                                         "metadata": dir_file_metadata[
                                             path]})
                elif os.path.isdir(path):
                    self.crawl_queue.put(path)

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
            messages = []

            while not (self.push_queue.empty()) and len(messages) < 10:
                message = self.push_queue.get()
                messages.append(message)

            put_messages(self.sqs_conn, messages, self.sqs_queue_name)
