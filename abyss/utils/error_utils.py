from tarfile import TarError, ReadError, CompressionError, StreamError, ExtractError, HeaderError
from zipfile import BadZipFile, LargeZipFile


def is_non_critical_funcx_error(exception: Exception) -> bool:
    """Determines whether an error is a non-critical funcX error.

    Parameters
    ----------
    exception : Exception

    Returns
    -------
    bool
    """
    critical_error_messages = {
        "waiting-for-ep",
        "waiting-for-nodes",
        "waiting-for-launch",
        "running",
        "success"
    }
    exception_message = str(exception)

    return exception_message in critical_error_messages


def is_critical_zip_error(exception: Exception) -> bool:
    """Determines whether an error is a critical error from the zipfile
    library.

    Parameters
    ----------
    exception : Exception

    Returns
    -------
    bool
    """
    critical_decompression_errors = {BadZipFile, LargeZipFile}

    return exception in critical_decompression_errors


def is_critical_gzip_error(exception: Exception) -> bool:
    """Determines whether an error is a critical error from the gzip
    library. Compatible with Python 3.7 or earlier.

    Parameters
    ----------
    exception : Exception

    Returns
    -------
    bool
    """
    critical_error_messages = {
        'Unknown compression method',
        'Not a gzipped file',
        "Compressed file ended before the end-of-stream marker was reached",
        "CRC check failed",
        "Incorrect length of data produced"
    }
    exception_message = str(exception)

    for error_message in critical_error_messages:
        if exception_message == error_message:
            return True

    return False


def is_critical_tar_error(exception: Exception) -> bool:
    """Determines whether an error is a critical error from the tarfile
    library.

    Parameters
    ----------
    exception : Exception

    Returns
    -------
    bool
    """
    critical_decompression_errors = {
        TarError, ReadError, CompressionError, StreamError,
        ExtractError, HeaderError
    }
    return exception in critical_decompression_errors


def is_critical_decompression_error(exception: Exception) -> bool:
    """Determines whether an error is a critical error from supported
    decompression libraries (zipfile, gzip, tarfile)

    Parameters
    ----------
    exception

    Returns
    -------
    bool
    """
    if is_critical_zip_error(exception):
        return True
    elif is_critical_gzip_error(exception):
        return True
    elif is_critical_tar_error(exception):
        return True
    else:
        return False


def is_critical_oom_error(exception: Exception) -> bool:
    """Determines whether an error is a critical OOM error.

    Parameters
    ----------
    exception : Exception

    Returns
    -------
    bool
    """
    critical_error_messages = {
        "[Errno 28] No space left on device"
    }
    exception_message = str(exception)

    return exception_message in critical_error_messages


if __name__ == "__main__":
    print(OSError('Unknown compression method'))