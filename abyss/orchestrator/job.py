from __future__ import annotations

import os
from enum import Enum
from collections import deque

REQUIRED_JOB_PARAMETERS = [
    ("file_path", str),
    ("compressed_size", int)
]


class JobStatus(Enum):
    UNPREDICTED = "UNPREDICTED"
    PREDICTED = "PREDICTED"
    SCHEDULED = "SCHEDULED"
    PREFETCHING = "PREFETCHING"
    PREFETCHED = "PREFETCHED"
    DECOMPRESSING = "DECOMPRESSING"
    DECOMPRESSED = "DECOMPRESSED"
    CRAWLING = "CRAWLING"
    CONSOLIDATING = "CONSOLIDATING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class Job:
    def __init__(self, file_path=None, file_id=None,
                 compressed_size=None,
                 decompressed_size=None, total_size=None,
                 worker_id=None, transfer_path=None,
                 decompress_path=None, funcx_decompress_id=None,
                 funcx_crawl_id=None, status=None,
                 child_jobs=None, metadata=None):
        self.file_path: str = file_path
        self.file_id: str = file_id
        self.compressed_size: int = compressed_size
        self.decompressed_size: int = decompressed_size
        self.total_size: int = total_size
        self.worker_id: str = worker_id
        self.transfer_path: str = transfer_path
        self.decompress_path: str = decompress_path
        self.funcx_decompress_id: str = funcx_decompress_id
        self.funcx_crawl_id: str = funcx_crawl_id
        self.status: JobStatus = status

        self.child_jobs: dict = child_jobs if child_jobs else dict()
        self.metadata: dict = metadata if metadata else dict()

    @staticmethod
    def validate_dict_params(job_params: dict) -> None:
        """Ensures dictionary of job parameters contains
        necessary parameters.

        Parameters
        ----------
        job_params : dict
            Dictionary containing parameters for Worker object.

        Returns
        -------
            Returns None if parameters are valid, raises error if
            invalid.
        """
        try:
            for parameter_name, parameter_type in REQUIRED_JOB_PARAMETERS:
                parameter = job_params[parameter_name]
                assert isinstance(parameter, parameter_type)
        except AssertionError:
            raise ValueError(
                f"Job parameter {parameter_name} is not of type {parameter_type}")
        except KeyError:
            raise ValueError(
                f"Job parameter {parameter_name} not found")

    @staticmethod
    def from_dict(job_params: dict) -> Job:
        """Factory method for creating Job objects from dictionary.

        Parameters
        ----------
        job_params : dict
            Valid dictionary with job parameters.

        Returns
        -------
        job : Job
            Job object created from job_params.
        """
        Job.validate_dict_params(job_params)

        child_jobs = dict()
        if "child_jobs" in job_params:
            child_job_dicts = job_params["child_jobs"]

            for file_path, child_job_dict in child_job_dicts.items():
                child_jobs[file_path] = (Job.from_dict(child_job_dict))

        if "status" in job_params:
            job_params["status"] = JobStatus[job_params["status"]]

        job = Job(**job_params)
        job.child_jobs = child_jobs

        return job

    @staticmethod
    def to_dict(job: Job) -> dict:
        """Serializes a Job object into a dictionary.

        Parameters
        ----------
        job : Job
            Job object to serialize.

        Returns
        -------
        job_dict : dict
            Dictionary of serialized job.
        """
        job_dict = {
            "file_path": job.file_path,
            "file_id": job.file_id,
            "compressed_size": job.compressed_size,
            "decompressed_size": job.decompressed_size,
            "total_size": job.total_size,
            "worker_id": job.worker_id,
            "transfer_path": job.transfer_path,
            "decompress_path": job.decompress_path,
            "funcx_decompress_id": job.funcx_decompress_id,
            "funcx_crawl_id": job.funcx_crawl_id,
            "status": job.status.value,
            "metadata": job.metadata
        }

        child_jobs = job.child_jobs
        child_job_dicts = dict()

        for file_path, child_job in child_jobs.items():
            child_job_dicts[file_path] = (Job.to_dict(child_job))

        job_dict["child_jobs"] = child_job_dicts

        return job_dict

    def bfs_iterator(self, include_root=False):
        """Breadth-first iterator through Job tree.

        Returns
        -------

        """
        iteration_queue = deque()

        if include_root:
            iteration_queue.append(self)
        else:
            for child_job in self.child_jobs.values():
                iteration_queue.append(child_job)

        while len(iteration_queue):
            job = iteration_queue.popleft()

            for child_job in job.child_jobs.values():
                iteration_queue.append(child_job)

            yield job

    def dfs_iterator(self, include_root=False):
        """Depth-first iterator through Job tree.

        Returns
        -------

        """
        iteration_queue = deque()

        if include_root:
            iteration_queue.append(self)
        else:
            for child_job in self.child_jobs.values():
                iteration_queue.append(child_job)

        while len(iteration_queue):
            job = iteration_queue.pop()

            for child_job in job.child_jobs.values():
                iteration_queue.append(child_job)

            yield job

    def to_queue(self, order="bfs", include_root=False):
        """Returns

        Parameters
        ----------
        order : str
            Order fo queue to return. Either "bfs" for breadth-first or
            "dfs" for depth-first.
        include_root : bool
            Whether to include the root node in queue.

        Returns
        -------

        """
        queue = deque()
        
        if order == "bfs":      
            for job_node in self.bfs_iterator(include_root=include_root):
                queue.append(job_node)
        elif order == "dfs":
            for job_node in self.dfs_iterator(include_root=include_root):
                queue.append(job_node)
        else:
            raise ValueError(f"{order} is invalid order type")
        
        return queue

    # This may get changed depending on how/when we decide to delete compressed files
    def calculate_total_size(self):
        """Calculates the total size of a Job tree. The total size is
        calculated by finding the max amount of space the job will take
        up when being processed. When the root Job is decompressed, both
        the compressed and decompressed file exist on the worker. Then
        the compressed file is deleted. For child Jobs, the compressed
        file and decompressed file remain on the file system. Therefore
        the max amount of space is max(compressed + decompressed size,
        decompressed size of all Job nodes).

        Returns
        -------

        """
        max_total_size = self.compressed_size + self.decompressed_size
        curr_size = self.decompressed_size

        for job_node in self.bfs_iterator():
            if job_node.status == JobStatus.FAILED:
                continue

            curr_size += job_node.decompressed_size
            job_node.calculate_total_size()

        self.total_size = max(max_total_size, curr_size)

    def consolidate_metadata(self) -> dict:
        """Recursively consolidates metadata of root Job and child Jobs.

        Returns
        -------
        consolidated_metadata : dict
            Combined metadata entries of root Jobs and child Jobs.
        """
        consolidated_metadata = {
            "compressed_path": self.file_path,
            "root_path": self.metadata["root_path"],
            "metadata": [],
            "files": [],
            "decompressed_size": 0
        }

        root_path = self.metadata["root_path"]
        for file_path, file_metadata in self.metadata["metadata"].items():
            metadata = file_metadata
            metadata["file_path"] = file_path

            if file_path == "":
                child_file_path = root_path
            else:
                child_file_path = os.path.join(root_path, file_path)

            if child_file_path in self.child_jobs:
                child_job = self.child_jobs[child_file_path]
                child_job_metadata = child_job.consolidate_metadata()

                metadata["compressed_metadata"] = child_job_metadata

            consolidated_metadata["metadata"].append(file_metadata)
            consolidated_metadata["files"].append(file_path)
            consolidated_metadata["decompressed_size"] += file_metadata["physical"]["size"]

        return consolidated_metadata


if __name__ == "__main__":
    job_dict = {
        "file_path": "/test",
        "compressed_size": 10,
        "decompressed_size": 20,
        "status": JobStatus.UNPREDICTED.value,
        "child_jobs": [
            {
                "file_path": "/test/1",
                "compressed_size": 5,
                "decompressed_size": 10
            },
            {
                "file_path": "/test/2",
                "compressed_size": 3,
                "decompressed_size": 6
            }
        ]
    }
    job_dict2 = {
        "file_path": "/nice",
        "compressed_size": 10,
        "decompressed_size": 20,
        "child_jobs": [
            {
                "file_path": "/test/1",
                "compressed_size": 5,
                "decompressed_size": 10
            },
            {
                "file_path": "/test/2",
                "compressed_size": 3,
                "decompressed_size": 6
            }
        ]
    }
    x = {'file_path': '/UMich/download/DeepBlueData_79407x76d/fig01.tar.gz', 'compressed_size': 38664, 'decompressed_size': 106857, 'total_size': 145521, 'worker_id': 'c9a8d41d-2ff7-44f9-a441-36c1c7386c16', 'transfer_path': '/home/tskluzac/ryan/deep_blue_data//UMich/download/DeepBlueData_79407x76d/fig01.tar.gz', 'decompress_path': '/home/tskluzac/ryan/results/fig01.tar', 'funcx_decompress_id': None, 'funcx_crawl_id': None, 'status': 'CONSOLIDATING', 'metadata': {'root_path': 'fig01.tar', 'metadata': {'': {'physical': {'size': 102400, 'extension': '.tar', 'is_compressed': True}}}}, 'child_jobs': {'fig01.tar': {'file_path': 'fig01.tar', 'compressed_size': 102400, 'decompressed_size': 7, 'total_size': 102407, 'worker_id': None, 'transfer_path': '/home/tskluzac/ryan/results/fig01.tar', 'decompress_path': '/home/tskluzac/ryan/results/fig01', 'funcx_decompress_id': None, 'funcx_crawl_id': None, 'status': 'CRAWLING', 'metadata': {'root_path': 'fig01', 'metadata': {'fig01/._plot_spectral_albedo.py': {'physical': {'size': 210, 'extension': '.py', 'is_compressed': False}}, 'fig01/._re_164um_0bc.txt': {'physical': {'size': 206, 'extension': '.txt', 'is_compressed': False}}, 'fig01/._re_164um_100bc.txt': {'physical': {'size': 206, 'extension': '.txt', 'is_compressed': False}}, 'fig01/._re_55um_0bc.txt': {'physical': {'size': 206, 'extension': '.txt', 'is_compressed': False}}, 'fig01/._re_55um_100bc.txt': {'physical': {'size': 206, 'extension': '.txt', 'is_compressed': False}}, 'fig01/._snicar_spectral_snow_albedo.pdf': {'physical': {'size': 233, 'extension': '.pdf', 'is_compressed': False}}, 'fig01/plot_spectral_albedo.py': {'physical': {'size': 2171, 'extension': '.py', 'is_compressed': False}}, 'fig01/re_164um_0bc.txt': {'physical': {'size': 12726, 'extension': '.txt', 'is_compressed': False}}, 'fig01/re_164um_100bc.txt': {'physical': {'size': 12725, 'extension': '.txt', 'is_compressed': False}}, 'fig01/re_55um_0bc.txt': {'physical': {'size': 12726, 'extension': '.txt', 'is_compressed': False}}, 'fig01/re_55um_100bc.txt': {'physical': {'size': 12725, 'extension': '.txt', 'is_compressed': False}}, 'fig01/snicar_spectral_snow_albedo.pdf': {'physical': {'size': 28293, 'extension': '.pdf', 'is_compressed': False}}}}, 'child_jobs': {}}}}

    y = Job.from_dict(x)

    print(y.to_queue(include_root=True))
    # print(y.consolidate_metadata())



