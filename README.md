# Abyss
Abyss is a distributed system for decompressing and processing large compressed data.  
  
The purpose of Abyss is to decompress and process compressed data as quickly as possible given both computing and 
storage constraints. 

# Architecture
Compressed data first begins at a storage unit with a Globus endpoint. Users pass the compressed data to be processed as 
well as **worker** information to Abyss. **Workers** are both storage and compute units with some sort of restriction on 
maximum available storage. Abyss then creates an **orchestrator** to decompress and process each compressed file on the 
given workers. 

The orchestration process consists of 7 steps: prediction, batching, dispatching, prefetching, decompressing, crawling, 
and metadata consolidation, all of which happen asynchronously.   
  
First, the decompressed size of each compressed file is predicted.  
  
Then, the compressed files are broken up into batches for each worker, such that the total decompressed size of each batch is less than the 
amount of available space on the worker.  
  
Then, each item in a batch is organized in the order that it is to be dispatched for processing on a worker. Then, each item is prefetched (transferred) to the worker from the initial 
storage location.  
  
Then, each compressed file is decompressed onto the worker. Any files that encounter OOM errors while 
decompressing will be resent to the orchestrator for reprocessing with a larger decompressed size estimation.  
  
Then, each decompressed file will then be crawled and physical file metadata (e.g. size, name, extension) will be 
collected. Additionally, crawlers can utilize **groupers** to generate additional file metadata during crawl time.  
  
Finally, the file metadata for each file is consolidated to be returned to the user.

## Predictors
Predictors utilize machine learning to predict the decompressed size of a file using the compressed size of compressed 
data. The models for predictors are individually created for each type of compressed file. Supported compressed 
extensions include `.tar`, `.gz`, `.zip`.  
  
## Batchers
Batchers take the list of compressed files and their decompressed sizes as well as worker information and divides up the 
compressed files amongst each worker for processing. The size of each batch is constrained by the maxmimum amount of 
storage space available on the worker that is to process the batch. Therefore, the total decompressed size of each batch 
is less than the amount of available space on the worker. Current batchers include the `knapsack` and `mmd` batcher.

### Knapsack Batcher
The Knapsack Batcher utilizes the 0-1 knapsack algorithm to sequentially maximize batch size for each worker. In this 
scenario, this algorithm is actually a heuristic as it maximizes each individual batch size but may not maximize the 
batch size for all batches. This algorithm operates in `O(N*C/I)` time and space, where `N` is the number of compressed 
files to batch, `C` is the capacity of the worker to create a batch for in bytes, and `I` is a user-defined variable 
called the **capacity interval**. The capacity interval is used to reduce the size and space complexity of this 
heuristic, as `C` is often in the range of `10^9-10^11`. However, it results in a maximum error of `N*I`. It should be 
noted that this batcher optimizes used space but may not result in "fair" job distribution.  

### MMD Batcher
The MMD batcher is a greedy algorithm which aims to minimize the maximum difference mean batch size difference between 
workers. This batcher aims to be a more "fair" approach for batching. 

## Dispatchers
Dispatchers order the items within a batch into the order in which they should be processed.

### LIFO Dispatcher
The LIFO Dispatcher simply treats the batch as a LIFO queue.

## Prefetcher
The prefetcher concurrently transfers files between two Globus endpoints.

## Decompressors
Decompressors decompress compressed files on workers. Currently, decompressors are invoked using a funcX function, 
meaning that decompressors currently must only use standard Python libraries. Current decompressors include `.tar`, 
`.gz`, `.zip`

## Crawlers
Crawlers crawl through decompressed directories and aggregate physical file metadata. Crawlers can additionally support 
the usage of **Groupers**, which can generate more physical file metadata. Supported crawlers include the Globus crawler.

### Globus Crawler    
The Globus crawler crawls directories via a Globus endpoint.

# TODOs:
- ~~Clean up imports~~

## Orchestrator
~~- Add logic to handle crawler metadata
    - ~~Consolidate metadata for a single directory into one dictionary and push to SQS~~
    ~~- How do we want to handle recursively compressed data (like `.tar.gz`)?
        - Add metadata parameter to help with keeping track of metadata when resubmitting tasks
        - It could be costly to re-transfer files if we've already decompressed a file on a worker
        - Decompressing additional data could result in OOM errors
        - Most likely will need to add additional logic in orchestrator to reprocess data.~~
            - ~~I think the easiest way to add the logic is to create a `Job` class that holds information like the 
            status, paths to crawl, etc. That way, when reprocessing files, we can just modify the `Job` instance to deal 
            with recursively compressed data.~~
- ~~Add methods to push status updates to postgresql~~
    - ~~Add `get_status` path to flask~~
- For small amounts of files, the orchestrator may terminate prematurely because the kill condition is determined by whether 
job queues are empty and sometimes they appear empty when jobs are being processed by threads
- Create sdk
- Figure out how/when to delete compressed/decompressed files
    - Figure out how to delete any empty directories left behind after clearing out compressed data
        - Might be better to just use a file name to uuid mapping to avoid needing to create the nested directory structures
        - As files are being decompressed, there may be a time where both the compressed and decompressed file exist, so 
        how to account for that needs to be figured out
- ~~Improve locking performance~~
    - ~~Tasks some tasks that require locks take too long and end up blocking other processes. Either switch to queues or
    choose better locations to lock.~~
## Predictors
- ~~Create `.zip` predictor~~
- Switch to `Keras` models instead of `sklearn` models
    - `Keras` models provide the opportunity to use custom loss functions
    - Determine optimum loss function to balance space vs time tradeoff (models that overpredict incur losses in space 
    but models that underpredict incur losses in processing time as they need to be reprocessed).
## Scheduler (Batcher + Dispatcher)
- ~~Create `Scheduler` class, which internally manages both a `Batcher` and `Dispatcher`~~
- ~~Add internal method to `Batcher` to update the amount of available space on a worker~~
- Create new dispatching strategies
    - ~~Potential dispatchers include a max first dispatcher, a min first dispatcher, and a max-min dispatcher (which 
    alternates between large and small jobs)~~
    - Determine the effect of using these dispatchers over a LIFO dispatcher. Might not have a super large benefit since 
    funcX internally handles scheduling for nodes on workers
- ~~Delete jobs from batcher once they've been scheduled into batches~~
- ~~Write a ton of tests~~
## Prefetcher
- Handle failed transfers
    - Investigate the difference between "fatal" and "non-fatal" error.
    - How does Globus clean up failed transfers?
## Crawlers   
- ~~Improve file throughput of crawler~~
    - ~~Pushing to SQS takes an awfully long time, perhaps just spinning up more threads will solve the issue.~~
    - ~~It might be better just for the crawler to return a massive metadata dictionary~~
        - ~~Results can just be returned via funcX and will reduce in significantly less overhead from using SQS~~
- Handle crawl fails
    - Perhaps add a secondary entry in metadata dictionaries for failed paths
- Determine if we want to use groups
    - Groups can be determined post crawl since only file names are used to determine group membership.
    - There is no real reason to process groups as one unit (if they are even processed at all).