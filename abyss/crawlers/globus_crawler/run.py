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
                                    job_node.decompress_dir,
                                    grouper_name,
                                    max_crawl_threads=max_crawl_threads)

            metadata = crawler.crawl()
            job_node.metadata = metadata

    return Job.to_dict(job)


if __name__ == "__main__":
    import funcx

    fx = funcx.FuncXClient()
    print(fx.register_function(run_crawler,
                               container_uuid="6daadc1b-c99b-47c4-b438-1fb6971f94ff"))