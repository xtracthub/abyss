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

        os.remove(job_node.transfer_path)

    return Job.to_dict(job)


if __name__ == "__main__":
    import funcx
    fx = funcx.FuncXClient()
    print(fx.register_function(run_decompressor, container_uuid="6daadc1b-c99b-47c4-b438-1fb6971f94ff"))
    print(fx.register_function(run_crawler, container_uuid="6daadc1b-c99b-47c4-b438-1fb6971f94ff"))
    # x = {'file_path': '/UMich/download/DeepBlueData_79407x76d/fig06.tar.gz', 'compressed_size': 115471, 'decompressed_size': 319130, 'total_size': 434601, 'worker_id': '944d2460-5db7-4bb2-b141-a9fb24c910c1', 'transfer_path': '/home/tskluzac/ryan/deep_blue_data//UMich/download/DeepBlueData_79407x76d/fig06.tar.gz', 'decompress_path': '/home/tskluzac/ryan/results/fig06.tar', 'funcx_decompress_id': None, 'funcx_crawl_id': None, 'status': 'CONSOLIDATING', 'metadata': {'root_path': 'fig06.tar', 'metadata': {'': {'physical': {'size': 245760, 'extension': '.tar', 'is_compressed': True}}}}, 'child_jobs': {'fig06.tar': {'file_path': 'fig06.tar', 'compressed_size': 245760, 'decompressed_size': 41, 'total_size': 245801, 'worker_id': None, 'transfer_path': '/home/tskluzac/ryan/results/fig06.tar', 'decompress_path': '/home/tskluzac/ryan/results/fig06', 'funcx_decompress_id': None, 'funcx_crawl_id': None, 'status': 'DECOMPRESSED', 'metadata': {}, 'child_jobs': {}}}}
    # print(run_crawler(x, "AgvKvXpGaDNYoNyE0p3p4q8BwnNvBn2WBK5JDkw05nBrawwnpNIzCQ3JBpNEQPK1DgyBB1YlYq82pEi9V9xO4HBvg6",
    #             "3f487096-811c-11eb-a933-81bbe47059f4", ""))
