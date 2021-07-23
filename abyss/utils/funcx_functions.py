import os
import funcx

LOCAL_CRAWLER_FUNCX_UUID = "2da9bfda-46aa-4041-9441-242c2881ad7a"
GLOBUS_CRAWLER_FUNCX_UUID = "3563bdeb-115d-4c3a-9c70-6ee8d55746a3"
DECOMPRESSOR_FUNCX_UUID = "ee2e8e53-abc7-4da7-a080-269abc6df874"
PROCESS_HEADER_FUNCX_UUID = "388128cf-79a4-4963-9e52-9c46cedae7ee"
# LOCAL_CRAWLER_FUNCX_UUID = "cdad46cc-4ef9-4893-8375-08218e39902d"
# GLOBUS_CRAWLER_FUNCX_UUID = "39bccae7-312c-4954-bb56-1efdee161e66"
# DECOMPRESSOR_FUNCX_UUID = "ea2aab1f-47ce-460c-befe-9cdf27883ca6"
# PROCESS_HEADER_FUNCX_UUID = "03d6d041-6335-4ac2-a09d-eba69c0bc1bd"


def run_globus_crawler(job_dict: dict, transfer_token: str, globus_eid: str,
                       grouper_name: str, max_crawl_threads=2):
    import os
    import shutil
    import sys
    sys.path.insert(0, "/")
    from abyss.orchestrator.job import Job, JobStatus
    from abyss.crawlers.globus_crawler.globus_crawler import GlobusCrawler

    job = Job.from_dict(job_dict)

    for job_node in job.bfs_iterator(include_root=True):
        if job_node.status == JobStatus.DECOMPRESSED:
            print(job_node.decompress_path)

            crawler = GlobusCrawler(transfer_token,
                                    globus_eid,
                                    job_node.decompress_path,
                                    grouper_name,
                                    max_crawl_threads=max_crawl_threads)

            metadata = crawler.crawl()
            job_node.metadata = metadata
            job_node.status = JobStatus.CRAWLING

            if os.path.exists(job_node.decompress_path):
                if os.path.isfile(job_node.decompress_path):
                    os.remove(job_node.decompress_path)
                    # logger.error(f"REMOVING FILE {job_node.decompress_path}")
                else:
                    shutil.rmtree(job_node.decompress_path)
                    # logger.error(f"REMOVING DIRECTORY {job_node.decompress_path}")

    return Job.to_dict(job)


def run_local_crawler(job_dict: bytes, grouper_name: str, max_crawl_threads=1):
    import logging
    import os
    import time
    import zlib
    import json
    import sys
    import shutil
    sys.path.insert(0, "/")
    from abyss.orchestrator.job import Job, JobStatus
    from abyss.crawlers.local_crawler.local_crawler import LocalCrawler
    from abyss.definitions import ROOT_DIR

    logger = logging.getLogger(__name__)
    #f_handler = logging.FileHandler(f'/project2/chard/skluzacek/ryan-data/abyss/file.log')
    f_handler = logging.FileHandler(f'/home/tskluzac/ryan/abyss/file.log')
    f_handler.setLevel(logging.ERROR)
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

    job = Job.from_dict(json.loads(zlib.decompress(job_dict).decode()))
    logger.error(f"LATENCY CRAWLING {Job.to_dict(job)}")
    logger.error(f"LATENCY CRAWLING TIME {job.file_id} {time.time()}")

    for job_node in job.bfs_iterator(include_root=True):
        if job_node.status == JobStatus.DECOMPRESSED:
            logger.error(f"CRAWLING {job_node.decompress_path}")
            crawler = LocalCrawler(job_node.decompress_path,
                                   grouper_name,
                                   max_crawl_threads=max_crawl_threads)

            metadata = crawler.crawl()
            job_node.metadata = metadata
            job_node.status = JobStatus.CRAWLING

        logger.error(f"LATENCY CRAWLING DELETING FILES TIME {job.file_id} {time.time()}")
        if job_node.decompress_path is not None and os.path.exists(job_node.decompress_path):
            if os.path.isfile(job_node.decompress_path):
                os.remove(job_node.decompress_path)
                logger.error(f"REMOVING FILE {job_node.decompress_path}")
            else:
                shutil.rmtree(job_node.decompress_path)
                logger.error(f"REMOVING DIRECTORY {job_node.decompress_path}")
        logger.error(f"FINISHED CRAWLING {job_node.file_path}")
        logger.error(f"LATENCY CRAWLING DELETING FILES TIME DONE {job.file_id} {time.time()}")

    logger.error(f"LATENCY DONE CRAWLING {Job.to_dict(job)}")
    logger.error(f"LATENCY CRAWLING TIME DONE {job.file_id} {time.time()}")
    return zlib.compress(json.dumps(Job.to_dict(job)).encode(), level=9)


def get_directory_size(start_path):
    """Gets total size of directory.

    Parameters
    ----------
    start_path : str
        Path to directory to get size of.

    Returns
    -------
    dir_size : int
        Size of start_path.
    """
    dir_size = 0
    for dir_path, dir_names, file_names in os.walk(start_path):
        for file_name in file_names:
            file_path = os.path.join(dir_path, file_name)
            dir_size += os.path.getsize(file_path)

    return dir_size


def run_decompressor(job_dict: bytes, decompress_dir: str):
    """Iterates through a Job and recursively decompresses files.

    Parameters
    ----------
    job_dict : dict
        Job dictionary to iterate through.
    decompress_dir : str
        Location on worker to decompress files to.

    Returns
    -------

    """
    import json
    import zlib
    import os
    import sys
    import time
    import logging
    from shutil import rmtree
    sys.path.insert(0, "/")
    from abyss.orchestrator.job import Job, JobStatus
    from abyss.utils.decompressors import decompress
    from abyss.utils.error_utils import is_critical_oom_error, is_critical_decompression_error
    from abyss.utils.funcx_functions import get_directory_size
    from abyss.definitions import ROOT_DIR
    job = Job.from_dict(json.loads(zlib.decompress(job_dict).decode()))

    logger = logging.getLogger(__name__)
    #f_handler = logging.FileHandler(f'/project2/chard/skluzacek/ryan-data/abyss/file.log')
    f_handler = logging.FileHandler(f'/home/tskluzac/ryan/abyss/file.log')
    f_handler.setLevel(logging.ERROR)
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)

    logger.error(f"LATENCY DECOMPRESSING {Job.to_dict(job)}")
    logger.error(f"LATENCY DECOMPRESSING START TIME {job.file_id} {time.time()}")

    job_nodes = job.to_queue(include_root=True)

    while len(job_nodes):
        job_node = job_nodes.popleft()

        file_path = job_node.transfer_path
        decompress_type = os.path.splitext(job_node.file_path)[1][1:]
        logger.error(f"DECOMPRESSING {file_path}")

        if job_node.status in [JobStatus.FAILED, JobStatus.SUCCEEDED]:
            continue

        try:
            if decompress_type == "zip":
                full_extract_dir = os.path.join(decompress_dir,
                                                job_node.file_id)

                try:
                    os.mkdir(full_extract_dir)
                except:
                    pass
                decompress(file_path, decompress_type, full_extract_dir)
            elif decompress_type == "tar":
                full_extract_dir = os.path.join(decompress_dir,
                                                job_node.file_id)
                decompress(file_path, decompress_type, full_extract_dir)
            elif decompress_type == "gz":
                extract_dir = os.path.join(os.path.join(decompress_dir, job_node.file_id),
                                           os.path.basename(job_node.file_path[:-3]))
                full_extract_dir = os.path.dirname(extract_dir)

                if not os.path.exists(os.path.dirname(extract_dir)):
                    os.makedirs(os.path.dirname(extract_dir))

                decompress(file_path, decompress_type, extract_dir)

            job_node.decompress_path = full_extract_dir

            logger.error(f"DECOMPRESSED {file_path} TO {full_extract_dir}")

            for child_job in job_node.child_jobs.values():
                # TODO: Fix this gross if statement. We might want to decompress
                # gz files into a directory
                if os.path.basename(full_extract_dir) == child_job.file_path:
                    child_job.transfer_path = full_extract_dir
                else:
                    child_job.transfer_path = os.path.join(decompress_dir,
                                                           child_job.file_path)

            if job_node.status == JobStatus.PREFETCHED:
                job_node.status = JobStatus.DECOMPRESSING

            logger.error(f"REMOVING {job_node.transfer_path}")
            logger.error(f"LATENCY DECOMPRESSING DELETING FILES TIME {job.file_id} {time.time()}")
            os.remove(job_node.transfer_path)
            logger.error(f"LATENCY DECOMPRESSING DELETING FILES TIME DONE {job.file_id} {time.time()}")
            logger.error(f"FINISHED DECOMPRESSING {job_node.file_path}")

        except Exception as e:
            logger.error(f"ERROR TYPE {e}")
            logger.error(f"CAUGHT ERROR", exc_info=True)
            if is_critical_decompression_error(e):
                logger.error("HANDLED DECOMPRESSION ERROR")
                if job_node.status == JobStatus.PREFETCHED:
                    job_node.status = JobStatus.FAILED
                    job_node.error = str(e)
                logger.error(f"LATENCY DECOMPRESSING DELETING FILES TIME {job.file_id} {time.time()}")
                os.remove(job_node.transfer_path)

                if os.path.exists(full_extract_dir):
                    rmtree(full_extract_dir)
                logger.error(f"LATENCY DECOMPRESSING DELETING FILES TIME DONE {job.file_id} {time.time()}")
            elif is_critical_oom_error(e):
                logger.error("PROCESSING OOM ERROR")
                decompressed_size = get_directory_size(full_extract_dir)
                if decompressed_size > job_node.decompressed_size:
                    logger.error("FILE TOO LARGE")
                    logger.error(f"LATENCY DECOMPRESSING DELETING FILES TIME {job.file_id} {time.time()}")
                    os.remove(job_node.transfer_path)
                    rmtree(full_extract_dir)
                    logger.error(f"LATENCY DECOMPRESSING DELETING FILES TIME DONE {job.file_id} {time.time()}")
                    for child_job in job_node.child_jobs:
                        job_nodes.remove(child_job)

                    job_node.status = JobStatus.UNPREDICTED
                    job_node.error = str(e)

                else:
                    logger.error("ATTEMPTING TO REPROCESS")
                    logger.error(f"LATENCY DECOMPRESSING DELETING FILES TIME {job.file_id} {time.time()}")
                    rmtree(full_extract_dir)
                    logger.error(f"LATENCY DECOMPRESSING DELETING FILES TIME DONE {job.file_id} {time.time()}")
                    job_nodes.appendleft(job_node)

            else:
                if job_node.status == JobStatus.PREFETCHED:
                    job_node.status = JobStatus.FAILED
                    job_node.error = str(e)
                logger.error(f"LATENCY DECOMPRESSING DELETING FILES TIME {job.file_id} {time.time()}")
                if job_node.transfer_path and os.path.exists(job_node.transfer_path):
                    os.remove(job_node.transfer_path)

                if os.path.exists(full_extract_dir):
                    rmtree(full_extract_dir)
                logger.error(f"LATENCY DECOMPRESSING DELETING FILES TIME DONE {job.file_id} {time.time()}")
    logger.error(f"LATENCY FINISHED DECOMPRESSING {Job.to_dict(job)}")
    logger.error(f"LATENCY FINISHED DECOMPRESSING TIME {job.file_id} {time.time()}")
    return zlib.compress(json.dumps(Job.to_dict(job)).encode(), level=9)


def process_job_headers(job_dict: bytes) -> bytes:
    """Takes a job object and reads the file header and determines the
    decompressed size of the job.

    Parameters
    ----------
    job_dict : dict
        Job dictionary.

    Returns
    -------
    dict
        Job dictionary containing the decompressed size.
    """
    import json
    import zlib
    import os
    import sys
    sys.path.insert(0, "/")
    from abyss.orchestrator.job import Job, JobStatus
    from abyss.utils.decompressors import get_zip_decompressed_size, get_tar_decompressed_size

    job = Job.from_dict(json.loads(zlib.decompress(job_dict).decode()))

    if job.status != JobStatus.UNPREDICTED_PREFETCHED:
        raise ValueError(f"Job {job.file_path} status is not PROCESSING_HEADERS")
    elif job.file_path.endswith(".zip"):
        decompressed_size = get_zip_decompressed_size(job.transfer_path)
    elif job.file_path.endswith(".tar"):
        decompressed_size = get_tar_decompressed_size(job.transfer_path)
    else:
        raise ValueError(f"Can not process headers of {job.file_path}")

    job.decompressed_size = decompressed_size
    os.remove(job.transfer_path)

    return zlib.compress(json.dumps(Job.to_dict(job)).encode(), level=9)


def register_funcs():
    fx = funcx.FuncXClient()
    container_uuid = fx.register_container("/home/tskluzac/jetstream-container.sif", "singularity")
    print(f"LOCAL_CRAWLER_FUNCX_UUID = \"{fx.register_function(run_local_crawler, container_uuid=container_uuid)}\"")
    print(f"GLOBUS_CRAWLER_FUNCX_UUID = \"{fx.register_function(run_globus_crawler, container_uuid=container_uuid)}\"")
    print(f"DECOMPRESSOR_FUNCX_UUID = \"{fx.register_function(run_decompressor, container_uuid=container_uuid)}\"")
    print(f"PROCESS_HEADER_FUNCX_UUID = \"{fx.register_function(process_job_headers, container_uuid=container_uuid)}\"")



if __name__ == "__main__":
    register_funcs()
