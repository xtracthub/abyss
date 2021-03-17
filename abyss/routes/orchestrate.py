import uuid

from flask import abort, Blueprint, request, current_app as app

from abyss.authentication.auth import authenticate
from abyss.orchestrator.abyss_orchestrator import AbyssOrchestrator
from abyss.utils.psql_utils import read_flask_db_config, \
    create_connection, create_table_entry, select_by_column
from abyss.utils.aws_utils import create_sqs_connection, \
    read_flask_aws_config

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
    compressed_files: list(dict)
        List of dictionaries containing compressed file path and
        compressed file size.
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

    psql_conn = create_connection(read_flask_db_config(app))
    db_entry = {"abyss_id": abyss_id, "client_id": client_id}
    create_table_entry(psql_conn, "abyss_status", **db_entry)

    sqs_conn = create_sqs_connection(**read_flask_aws_config(app))

    grouper = ""
    batcher = "mmd"
    dispatcher = "fifo"

    if "grouper" in orchestrator_params:
        grouper = orchestrator_params["grouper"]
    if "batcher" in orchestrator_params:
        batcher = orchestrator_params["batcher"]
    if "dispatcher" in orchestrator_params:
        dispatcher = orchestrator_params["dispatcher"]

    orchestrator = AbyssOrchestrator(
        abyss_id,
        orchestrator_params["globus_source_eid"],
        orchestrator_params["transfer_token"],
        orchestrator_params["compressed_files"],
        orchestrator_params["worker_params"],
        psql_conn,
        sqs_conn,
        grouper=grouper,
        batcher=batcher,
        dispatcher=dispatcher
    )

    orchestrator.start()

    return abyss_id


@orchestrate_api.route("/get_status", methods=["GET"])
def get_status():
    """Handles requests for retrieving orchestration status.

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
                                     **{"client_id": client_id,
                                        "abyss_id": abyss_id})

    if user_statuses:
        return user_statuses[0]
    else:
        abort(f"Abyss ID {abyss_id} does not match any crawls for user {client_id}")
