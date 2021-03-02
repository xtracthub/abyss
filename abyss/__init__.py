from flask import Flask

from abyss.config import Config
from abyss.routes.orchestrate import orchestrate_api
from abyss.utils.psql_utils import ABYSS_TABLES, read_flask_db_config, \
    create_connection, build_tables, table_exists


def create_app():
    application = Flask(__name__)

    application.config.from_object(Config)

    @application.before_first_request
    def create_tables():
        conn = create_connection(read_flask_db_config(application))
        build_tables(conn, ABYSS_TABLES)

    # Include the API blueprint
    application.register_blueprint(orchestrate_api)
    return application
