def run_crawler(base_path, globus_eid, grouper_name,
                max_crawl_threads=2, max_push_threads=4):
    import os
    from abyss.crawlers.globus_crawler.globus_crawler import GlobusCrawler
    from abyss.crawlers.utils.sqs_utils import create_sqs_connection

    crawler = GlobusCrawler(os.environ.get("TRANSFER_TOKEN"),
                            globus_eid,
                            base_path,
                            grouper_name,
                            None,
                            create_sqs_connection(os.environ.get("AWS_ACCESS"),
                                                  os.environ.get("AWS_SECRET"),
                                                  region_name="us-east-1"),
                            max_crawl_threads=max_crawl_threads,
                            max_push_threads=max_push_threads)
    return crawler.crawl(blocking=True)


if __name__ == "__main__":
    run_crawler("/Users/ryan/Documents/CS/abyss",
                "5ecf6444-affc-11e9-98d4-0a63aa6b37da",
                "")