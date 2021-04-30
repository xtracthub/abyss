import os
import threading
import time
import uuid
from queue import Queue
from abyss.crawlers.crawler import Crawler
from abyss.crawlers.groupers import get_grouper
from abyss.decompressors import is_compressed


LOCAL_CRAWLER_FUNCX_UUID = "a35e75b7-33d3-449c-b889-ea613fe3ecba"


class LocalCrawler(Crawler):
    def __init__(self, base_path: str, grouper_name: str,
                 max_crawl_threads=2):
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
        self.base_path = base_path
        self.max_crawl_threads = max_crawl_threads

        self.crawl_results = {"root_path": os.path.basename(base_path),
                              "metadata": {}}
        self.crawl_queue = Queue()
        self.crawl_threads_status = dict()
        self.grouper = get_grouper(grouper_name)

    def crawl(self) -> dict:
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
        self._start_crawl()

        return self.crawl_results

    def _start_crawl(self):
        """Internal blocking method for starting local crawl. Starts all
        threads and updates database with crawl status."""
        self.crawl_queue.put(self.base_path)

        crawl_threads = []
        for i in range(self.max_crawl_threads):
            thread_id = str(uuid.uuid4())
            thread = threading.Thread(target=self._thread_crawl,
                                      args=(thread_id,))
            thread.start()
            crawl_threads.append(thread)
            self.crawl_threads_status[thread_id] = "WORKING"

        self.crawl_status = "CRAWLING"

        for thread in crawl_threads:
            thread.join()

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

            for full_path in file_ls:
                if os.path.isfile(full_path):
                    path = full_path[len(self.base_path) + 1:]
                    extension = self.get_extension(full_path)
                    file_size = os.path.getsize(full_path)

                    dir_file_metadata[path] = {
                        "physical": {
                            "size": file_size,
                            "extension": extension,
                            "is_compressed": is_compressed(full_path)
                        }
                    }
                elif os.path.isdir(full_path):
                    self.crawl_queue.put(full_path)

            for path, metadata in dir_file_metadata.items():
                self.crawl_results["metadata"][path] = metadata


if __name__ == "__main__":
    crawler = LocalCrawler("/Users/ryan/Documents/CS/abyss",
                           "")
    print(crawler.crawl())