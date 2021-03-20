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

            # if os.path.isfile(job_node.decompress_path):
            #     os.remove(job_node.decompress_path)
            # else:
            #     shutil.rmtree(job_node.decompress_path)

    return Job.to_dict(job)


def run_local_crawler(job_dict: dict, grouper_name: str, max_crawl_threads=2):
    import os
    import shutil
    import sys
    sys.path.insert(0, "/")
    from abyss.orchestrator.job import Job, JobStatus
    from abyss.crawlers.local_crawler.local_crawler import LocalCrawler

    job = Job.from_dict(job_dict)

    for job_node in job.bfs_iterator(include_root=True):
        if job_node.status == JobStatus.DECOMPRESSED:

            crawler = LocalCrawler(job_node.decompress_path,
                                   grouper_name,
                                   max_crawl_threads=max_crawl_threads)

            metadata = crawler.crawl()
            job_node.metadata = metadata
            job_node.status = JobStatus.CRAWLING

            # if os.path.isfile(job_node.decompress_path):
            #     os.remove(job_node.decompress_path)
            # else:
            #     shutil.rmtree(job_node.decompress_path)

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

        # os.remove(job_node.transfer_path)

    return Job.to_dict(job)


if __name__ == "__main__":
    import funcx
    fx = funcx.FuncXClient()
    print(fx.register_function(run_decompressor, container_uuid="6daadc1b-c99b-47c4-b438-1fb6971f94ff"))
    print(fx.register_function(run_globus_crawler, container_uuid="6daadc1b-c99b-47c4-b438-1fb6971f94ff"))
    print(fx.register_function(run_local_crawler,container_uuid="6daadc1b-c99b-47c4-b438-1fb6971f94ff"))
    # x = {'file_path': '/UMich/download/DeepBlueData_pv63g053w/repro_200k_annotations.tar.gz', 'file_id': 'a8fbf8a7-0272-4af2-837f-b0e1c6c8cf05', 'compressed_size': 39726958, 'decompressed_size': 109794247, 'total_size': 317000857, 'worker_id': '86cf9bc8-1792-4f2f-92dd-2967c411d962', 'transfer_path': '/home/tskluzac/ryan/deep_blue_data/a8fbf8a7-0272-4af2-837f-b0e1c6c8cf05', 'decompress_path': '/home/tskluzac/ryan/results/a8fbf8a7-0272-4af2-837f-b0e1c6c8cf05', 'funcx_decompress_id': None, 'funcx_crawl_id': None, 'status': 'CONSOLIDATING', 'metadata': {'root_path': 'a8fbf8a7-0272-4af2-837f-b0e1c6c8cf05', 'metadata': {'repro_200k_annotations.tar': {'physical': {'size': 563630080, 'extension': '.tar', 'is_compressed': True}}}}, 'child_jobs': {'a8fbf8a7-0272-4af2-837f-b0e1c6c8cf05/repro_200k_annotations.tar': {'file_path': 'a8fbf8a7-0272-4af2-837f-b0e1c6c8cf05/repro_200k_annotations.tar', 'file_id': 'bf1354e9-583b-4570-ac26-cf6ab0a8a505', 'compressed_size': 563630080, 'decompressed_size': 207206610, 'total_size': 770836690, 'worker_id': None, 'transfer_path': '/home/tskluzac/ryan/results/a8fbf8a7-0272-4af2-837f-b0e1c6c8cf05/repro_200k_annotations.tar', 'decompress_path': '/home/tskluzac/ryan/results/bf1354e9-583b-4570-ac26-cf6ab0a8a505/repro_200k_annotations', 'funcx_decompress_id': None, 'funcx_crawl_id': None, 'status': 'DECOMPRESSED', 'metadata': {}, 'child_jobs': {}}}}
    # transfer_token = "AgvKvXpGaDNYoNyE0p3p4q8BwnNvBn2WBK5JDkw05nBrawwnpNIzCQ3JBpNEQPK1DgyBB1YlYq82pEi9V9xO4HBvg6"
    # eid = "3f487096-811c-11eb-a933-81bbe47059f4"
    # print(run_crawler(x, transfer_token, eid, ""))
