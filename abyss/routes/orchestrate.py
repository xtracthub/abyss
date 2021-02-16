import uuid
from flask import Blueprint, request, current_app as app
from abyss.authentication.auth import authenticate
from abyss.orchestrator.abyss_orchestrator import AbyssOrchestrator
from abyss.utils.psql_utils import read_flask_db_config, \
    create_connection, create_table_entry, select_by_column


orchestrate_api = Blueprint("orchestrate", __name__)


@orchestrate_api.route("/")
def root():
    return "hello world"


@orchestrate_api.route("/launch", methods=["POST"])
def launch():
    """Handles requests for starting orchestration jobs.

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
    groupers : str
        Name of groupers to group decompressed data.
    worker_params: list(dict)
        List of dictionary entries for each compute worker. Dictionary
        should include globus_dest_eid (Globus endpoint ID), funcx_eid
        (funcX endpoint ID), bytes_available (available storage in
        bytes), transfer_dir (directory to transfer files to),
        decompress_dir (directory to decompress data to).

    Returns
    -------
    abyss_id : str
        UUID of abyss job.
    """
    client_id = authenticate(request)

    orchestrator_params = request.json
    abyss_id = str(uuid.uuid4())

    conn = create_connection(read_flask_db_config(app))
    db_entry = {
        "abyss_id": abyss_id,
        "client_id": client_id,
        "status": None
    }
    create_table_entry(conn, "abyss_status", **db_entry)

    return abyss_id


@orchestrate_api.route("/get_status", methods=["GET"])
def get_status():
    """Handles requests for starting crawls.

    Request Parameters
    -------
    abyss_id : str
        ID for Abyss job.

    Returns
    -------
    abyss_status : dict
        Status of Abyss job.
    """
    client_id = authenticate(request)

    conn = create_connection(read_flask_db_config(app))
    status_params = request.json
    abyss_id = status_params["abyss_id"]

    user_statuses = select_by_column(conn, "abyss_status",
                                     **{"client_id": client_id})

    for user_status in user_statuses:
        if user_status["abyss_id"] == abyss_id:
            return user_status

    return f"Abyss ID {abyss_id} does not match any crawls for user {client_id}"
