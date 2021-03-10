def run_crawler(transfer_token, base_path, globus_eid, grouper_name,
                max_crawl_threads=2):
    import sys
    sys.path.insert(0, "/")
    from abyss.crawlers.globus_crawler.globus_crawler import GlobusCrawler

    crawler = GlobusCrawler(transfer_token,
                            globus_eid,
                            base_path,
                            grouper_name,
                            max_crawl_threads=max_crawl_threads)
    return crawler.crawl()


if __name__ == "__main__":
    import time
    t0 = time.time()
    print(run_crawler("Ago2vz44pB9bm58XvwVVWvpv3QJxe3W35V9oN1oPgPyao5NwkoHgCybQd7NMemmEBgVxBn095J0grGCKqEop2f1605",
              "/home/tskluzac/ryan/results/UmichIndoorCorridor2012.tar",
              "3f487096-811c-11eb-a933-81bbe47059f4",
                                                          "",
              1))
    print(time.time() - t0)