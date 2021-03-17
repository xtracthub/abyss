def run_crawler(job_dict: dict, transfer_token: str, globus_eid: str,
                grouper_name: str, max_crawl_threads=2):
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

        full_extract_dir = decompress(file_path, decompress_dir)
        job_node.decompress_path = full_extract_dir

        for child_job in job_node.child_jobs.values():
            # TODO: Fix this gross if statement. We might want to decompress
            # gz files into a directory
            if os.path.basename(full_extract_dir) == child_job.file_path:
                child_job.transfer_path = full_extract_dir
            else:
                child_job.transfer_path = os.path.join(full_extract_dir,
                                                       child_job.file_path)

        if job_node.status == JobStatus.PREFETCHED:
            job_node.status = JobStatus.DECOMPRESSING

    return Job.to_dict(job)


if __name__ == "__main__":
    import funcx
    fx = funcx.FuncXClient()
    print(fx.register_function(run_decompressor, container_uuid="6daadc1b-c99b-47c4-b438-1fb6971f94ff"))
    print(fx.register_function(run_crawler, container_uuid="6daadc1b-c99b-47c4-b438-1fb6971f94ff"))

