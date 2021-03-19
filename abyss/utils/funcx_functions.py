def run_crawler(job_dict: dict, transfer_token: str, globus_eid: str,
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

            crawler = GlobusCrawler(transfer_token,
                                    globus_eid,
                                    job_node.decompress_path,
                                    grouper_name,
                                    max_crawl_threads=max_crawl_threads)

            metadata = crawler.crawl()
            job_node.metadata = metadata
            job_node.status = JobStatus.CRAWLING

            if os.path.isfile(job_node.decompress_path):
                os.remove(job_node.decompress_path)
            else:
                shutil.rmtree(job_node.decompress_path)

    return Job.to_dict(job)


def run_decompressor(job_dict: dict, decompress_dir: str):
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
    import os
    import sys
    sys.path.insert(0, "/")
    from abyss.orchestrator.job import Job, JobStatus
    from abyss.decompressors import decompress
    job = Job.from_dict(job_dict)

    for job_node in job.bfs_iterator(include_root=True):
        file_path = job_node.transfer_path
        decompress_type = os.path.splitext(job_node.file_path)[1][1:]

        if decompress_type == "zip":
            full_extract_dir = os.path.join(decompress_dir,
                                            os.path.basename(file_path)[:-4])
            decompress(file_path, decompress_type, full_extract_dir)
        elif decompress_type == "tar":
            full_extract_dir = os.path.join(os.path.join(decompress_dir, job_node.file_id),
                                            os.path.basename(job_node.file_path[:-4]))
            decompress(file_path, decompress_type, full_extract_dir)
        elif decompress_type == "gz":
            full_extract_dir = os.path.join(os.path.join(decompress_dir, job_node.file_id),
                                            os.path.basename(job_node.file_path[:-3]))

            if not os.path.exists(os.path.dirname(full_extract_dir)):
                os.makedirs(os.path.dirname(full_extract_dir))

            decompress(file_path, decompress_type, full_extract_dir)

            full_extract_dir = os.path.dirname(full_extract_dir)

        job_node.decompress_path = full_extract_dir

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

        os.remove(job_node.transfer_path)

    return Job.to_dict(job)


if __name__ == "__main__":
    import funcx
    fx = funcx.FuncXClient()
    print(fx.register_function(run_decompressor, container_uuid="6daadc1b-c99b-47c4-b438-1fb6971f94ff"))
    print(fx.register_function(run_crawler, container_uuid="6daadc1b-c99b-47c4-b438-1fb6971f94ff"))
    x = {'file_path': '/UMich/download/DeepBlueData_79407x76d/fig01.tar.gz', 'file_id': '2b8cf032-61ad-4a04-a0af-63e88085201b', 'compressed_size': 38664, 'decompressed_size': 106857, 'total_size': 389863, 'worker_id': '861da5b4-01fd-4ce1-9b4a-dfeaa2f8192b', 'transfer_path': '/home/tskluzac/ryan/deep_blue_data/2b8cf032-61ad-4a04-a0af-63e88085201b', 'decompress_path': '/home/tskluzac/ryan/results/2b8cf032-61ad-4a04-a0af-63e88085201b', 'funcx_decompress_id': None, 'funcx_crawl_id': None, 'status': 'CONSOLIDATING', 'metadata': {'root_path': '2b8cf032-61ad-4a04-a0af-63e88085201b', 'metadata': {'fig01.tar.gz': {'physical': {'size': 102400, 'extension': '.gz', 'is_compressed': True}}}}, 'child_jobs': {'2b8cf032-61ad-4a04-a0af-63e88085201b/fig01.tar.gz': {'file_path': '2b8cf032-61ad-4a04-a0af-63e88085201b/fig01.tar.gz', 'file_id': '046fd8a1-6d37-4175-acc0-872e321854b0', 'compressed_size': 102400, 'decompressed_size': 283006, 'total_size': 385406, 'worker_id': None, 'transfer_path': None, 'decompress_path': None, 'funcx_decompress_id': None, 'funcx_crawl_id': None, 'status': 'PREFETCHED', 'metadata': {}, 'child_jobs': {}}}}