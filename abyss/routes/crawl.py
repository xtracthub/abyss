import uuid
from flask import Blueprint, request, current_app as app
from abyss.authentication.auth import authenticate
from abyss.orchestrator.abyss_orchestrator import AbyssOrchestrator


crawl_api = Blueprint("crawl", __name__)


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
        should include globus_dest_eid (Globus endpoint ID), funx_eid
        (funcX endpoint ID), bytes_available (available storage in
        bytes), decompress_dir (directory to decompress data to).

    Returns
    -------
    crawl_id : str
        UUID of crawl job.
    """
    client_id = authenticate(request)

    orchestrator_params = request.json

    return str(uuid.uuid4())
