import os
import threading
import time
import uuid
from queue import Queue
from xtract_sdk.packagers import Family
from abyss.crawler.crawler import Crawler
from abyss.grouper import get_grouper
from abyss.utils.sqs_utils import put_message
from abyss.utils.psql_utils import create_table_entry, update_table_entry, \
    select_by_column


class LocalCrawler(Crawler):
    def __init__(self, abyss_id: str, base_path: str, grouper_name: str,
                 max_crawl_threads: int, max_push_threads: int,
                 conn, sqs_queue_name: str,
                 aws_access: str, aws_secret: str, region_name: str):
        self.abyss_id = abyss_id
        self.base_path = base_path
        self.max_crawl_threads = max_crawl_threads
        self.max_push_threads = max_push_threads
        self.db_conn = conn
        self.sqs_queue_name = sqs_queue_name
        self.aws_access = aws_access
        self.aws_secret = aws_secret
        self.region_name = region_name

        self.crawl_id = str(uuid.uuid4())
        self.crawl_queue = Queue()
        self.push_queue = Queue()
        self.crawl_threads_status = dict()
        self.push_threads_status = dict()
        self.grouper = get_grouper(grouper_name)

    def crawl(self):
        threading.Thread(target=self._start_crawl).start()

        return self.crawl_id

    def get_status(self) -> str:
        crawl_status_entry = select_by_column(self.conn, "crawl_status",
                                              **{"crawl_id": self.crawl_id})

        return crawl_status_entry[0]["crawl_id"]

    def _start_crawl(self):
        self.crawl_queue.put(self.base_path)

        create_table_entry(self.conn, "crawl_status",
                           **{"crawl_id": self.crawl_id,
                              "crawl_status": "STARTING"})

        crawl_threads = []
        for _ in range(self.max_crawl_threads):
            thread = threading.Thread().start()
            crawl_threads.append(thread)
            self.crawl_threads_status[str(uuid.uuid4())] = "WORKING"

        update_table_entry(self.conn, "crawl_status",
                           {"crawl_id": self.crawl_id},
                           **{"crawl_status": "CRAWLING"})

        push_threads = []
        for _ in range(self.max_push_threads):
            thread = threading.Thread().start()
            push_threads.append(thread)
            self.push_threads_status[str(uuid.uuid4())] = "WORKING"

        for thread in crawl_threads:
            thread.join()

        update_table_entry(self.conn, "crawl_status",
                           {"crawl_id": self.crawl_id},
                           **{"crawl_status": "PUSHING"})

        for thread in push_threads:
            thread.join()

        update_table_entry(self.conn, "crawl_status",
                           {"crawl_id": self.crawl_id},
                           **{"crawl_status": "COMPLETE"})

    def _thread_crawl(self, thread_id):
        while True:
            if self.crawl_queue.empty():
                if all([status in ("IDLE", "FINISHED") for status in self.crawl_threads_status.values()]):
                    break
                else:
                    self.crawl_threads_status[thread_id] = "IDLE"
                    time.sleep(5)
                    pass

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
                elif os.path.isdir(path):
                    self.crawl_queue.put(path)

            families = self.grouper.group(file_ls)

            # TODO: Stolen from xtract crawler. Will have to be revisited later.
            if isinstance(families, list):
                for family in families:
                    fam = Family()

                    for group in family['groups']:
                        file_dict_ls = []
                        for fname in group['files']:
                            f_dict = {'path': fname,
                                      'metadata': dir_file_metadata[fname],
                                      'base_url': self.base_path}
                            file_dict_ls.append(f_dict)

                        fam.add_group(files=file_dict_ls,
                                      parser=group["parser"])

                    dict_fam = fam.to_dict()
                    dict_fam['metadata'][
                        'crawl_timestamp'] = time.time()

                    self.push_queue.put(dict_fam)

        self.crawl_threads_status[thread_id] = "FINISHED"

    def _thread_push(self, thread_id):
        while True:
            if self.push_queue.empty():
                if all([status in ("IDLE", "FINISHED") for status in self.crawl_threads_status.values()]):
                    if all([status in ("IDLE", "FINISHED") for status in self.push_threads_status.values()]):
                        break
                else:
                    self.push_threads_status[thread_id] = "IDLE"
                    time.sleep(5)
                    pass

            self.push_threads_status[thread_id] = "WORKING"
            message = self.push_queue.get()

            put_message(message, self.sqs_queue_name,
                        self.aws_access, self.aws_secret,
                        self.region_name)

        self.push_threads_status[thread_id] = "FINISHED"

    @staticmethod
    def get_extension(file_path):
        """Returns the extension of a filepath.
        Parameter:
        filepath (str): Filepath to get extension of.
        Return:
        extension (str): Extension of filepath.
        """
        extension = os.path.splitext(file_path)[1]

        return extension
