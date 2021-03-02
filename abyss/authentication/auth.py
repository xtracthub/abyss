from flask import abort, request, current_app as app
from globus_sdk import ConfidentialAppAuthClient


def authenticate(r: request) -> [str, abort]:
    """Authenticates a request using Globus Auth. If user is
    authenticated, return client_id from Globus introspection object,
    else abort.

    Parameters
    ----------
    r : request
        Flask request object as made by the user.

    Returns
    -------
    [str, abort]
        Returns client ID or abort message.
    """
    if "Authorization" not in r.headers:
        abort(401, "You must be logged in to perform this function.")
    else:
        token = request.headers.get("Authorization")
        token = str.replace(str(token), "Bearer ", "")
        conf_app = ConfidentialAppAuthClient(app.config["GLOBUS_CLIENT"],
                                             app.config["GLOBUS_KEY"])
        intro_obj = conf_app.oauth2_token_introspect(token)

        if "client_id" in intro_obj:
            return str(intro_obj["client_id"])
        else:
            abort(400, "Failed to authenticate user")
