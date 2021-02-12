import uuid
from flask import Blueprint, request, current_app as app
from abyss.authentication.auth import authenticate
from abyss.orchestrator.abyss_orchestrator import AbyssOrchestrator
from abyss.utils.psql_utils import create_connection, create_table_entry, select_by_column


crawl_api = Blueprint("crawl", __name__)


@crawl_api.route("/")
def root():
    return "hello world"


@crawl_api.route("/crawl", methods=["POST"])
def crawl():
    """Handles requests for starting crawls.

    Request Parameters
    -------
    globus_source_eid : str
        Globus endpoint ID of source data storage.
    auth_token : str
        Authorization token to access source Globus data storage.
    transfer_token : str
        Authorization token to transfer Globus files.
    compressed_files: list(str)
        List of compressed data to crawl and process.
    grouper : str
        Name of grouper to group decompressed data.
    worker_params: list(dict)
        List of dictionary entries for each compute worker. Dictionary
        should include globus_dest_eid (Globus endpoint ID), funcx_eid
        (funcX endpoint ID), bytes_available (available storage in
        bytes), transfer_dir (directory to transfer files to),
        decompress_dir (directory to decompress data to).

    Returns
    -------
    crawl_id : str
        UUID of crawl job.
    """
    client_id = authenticate(request)

    orchestrator_params = request.json
    crawl_id = str(uuid.uuid4())

    conn = create_connection(app)
    db_entry = {
        "crawl_id": crawl_id,
        "client_id": client_id,
        "crawl_status": None
    }
    create_table_entry(conn, "crawl", **db_entry)

    return crawl_id


@crawl_api.route("/get_crawl_status", methods=["GET"])
def get_crawl_status():
    """Handles requests for starting crawls.

    Request Parameters
    -------
    crawl_id : str
        ID for crawl job.

    Returns
    -------
    crawl_status : dict
        Status of crawl.
    """
    client_id = authenticate(request)

    conn = create_connection(app)
    crawl_status_params = request.json
    crawl_id = crawl_status_params["crawl_id"]

    user_crawls = select_by_column(conn, "crawl",
                                   **{"client_id": client_id})

    for user_crawl in user_crawls:
        if user_crawl["crawl_id"] == crawl_id:
            return user_crawl

    return f"Crawl ID {crawl_id} does not match any crawls for user {client_id}"
