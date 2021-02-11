from flask import Flask
from abyss.config import Config
from abyss.routes.crawl import crawl_api


def create_app():
    application = Flask(__name__)

    application.config.from_object(Config)

    # Include the API blueprint
    application.register_blueprint(crawl_api)
    return application
