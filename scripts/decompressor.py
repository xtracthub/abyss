import gzip
import os
import tarfile
import zipfile


def decompress_zip(file_path: str, extract_dir: str) -> None:
    """Decompresses .zip files into a directory.

    Parameters
    ----------
    file_path : str
        File path to .zip directory.
    extract_dir : str
        Path to directory to decompress to.
    """
    with zipfile.ZipFile(file_path, "r") as zip_f:
        zip_f.extractall(extract_dir)


def decompress_gz(file_path: str, extract_file: str) -> None:
    """Decompresses .gz files into a file.

    Parameters
    ----------
    file_path : str
        File path to .zip directory.
    extract_file : str
        Path to file to decompress to.
    """
    with gzip.open(file_path, "rb") as gz_f:
        with open(extract_file, "wb") as f:
            f.write(gz_f.read())


def decompress_tar(file_path: str, extract_dir: str) -> None:
    """Decompresses .tar files into a directory.

    Parameters
    ----------
    file_path : str
        File path to .tar directory.
    extract_dir : str
        Path to directory to decompress to.
    """
    with tarfile.open(file_path) as tar_f:
        tar_f.extractall(extract_dir)


def decompress_tar_gz(file_path: str, extract_dir: str) -> None:
    """Decompresses .tar.gz files into a directory.

    Parameters
    ----------
    file_path : str
        File path to .tar.gz directory.
    extract_dir : str
        Path to directory to decompress to.
    """
    with tarfile.open(file_path, "r:gz") as tar_gz_f:
        tar_gz_f.extractall(extract_dir)


def is_compressed(file_path: str) -> bool:
    """Determines whether a file is compressed based on file path.

    Parameters
    ----------
    file_path : str
        File path to file.

    Returns
    -------
    bool
        Whether file_path is a compressed file.
    """
    compressed_extensions = [".gz", ".tar", ".tar.gz", ".zip"]

    for extension in compressed_extensions:
        if file_path.endswith(extension):
            return True

    return False


def decompress(file_path: str, extract_dir: str) -> None:
    """Recursively decompresses compressed files.

    Parameters
    ----------
    file_path : str
        Path to compressed file to decompress
    extract_dir : str
        Path to directory to to extract file contents to. All compressed files
        with the exception of .gz files are extracted to a subdirectory inside
        extract_dir named after the compressed file (minus the extension). For
        example, if file_path was "tests/test.zip" and extract_dir was "test_results/",
        the contents of "tests/test_file.zip" would be in "test_results/test_file/".
        .gz files are simply decompressed into the extract_dir directory as a
        single file (minus the extension).

    """

    if is_compressed(file_path):
        if file_path.endswith(".zip"):
            full_extract_dir = os.path.join(extract_dir,
                                            os.path.basename(file_path)[:-4])
            decompress_zip(file_path, full_extract_dir)

        elif file_path.endswith(".tar.gz"):
            full_extract_dir = os.path.join(extract_dir,
                                            os.path.basename(file_path)[:-7])
            decompress_tar_gz(file_path, full_extract_dir)

        elif file_path.endswith(".tar"):
            full_extract_dir = os.path.join(extract_dir,
                                            os.path.basename(file_path)[:-4])
            decompress_tar(file_path, full_extract_dir)

        elif file_path.endswith(".gz"):
            full_extract_dir = os.path.join(extract_dir,
                                            os.path.basename(file_path)[:-3])
            decompress_gz(file_path, full_extract_dir)
        else:
            raise ValueError(f"{file_path} is not a compressed file")

        for path, subdirs, files in os.walk(full_extract_dir):
            for file in files:
                if is_compressed(file):
                    decompress(os.path.join(path, file), path)


if __name__ == "__main__":
    import time
    file_path = "Observation_Data.tar"
    full_extract_dir = "."

    t0 = time.time()
    with tarfile.open(file_path) as tar_f:
        print([tar_info.name for tar_info in tar_f.getmembers()])
    print(time.time() - t0)
