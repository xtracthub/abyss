from flask import Flask
from abyss.config import Config
from abyss.routes.crawl import crawl_api
from abyss.utils.psql_utils import ABYSS_TABLES, create_connection, \
    build_tables, table_exists


def create_app():
    application = Flask(__name__)

    application.config.from_object(Config)

    @application.before_first_request
    def create_tables():
        conn = create_connection(application)
        build_tables(conn, ABYSS_TABLES)

    # Include the API blueprint
    application.register_blueprint(crawl_api)
    return application
