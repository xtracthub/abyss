import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    GLOBUS_KEY = os.environ.get("GLOBUS_KEY")
    GLOBUS_CLIENT = os.environ.get("GLOBUS_CLIENT")

    DB_HOST = os.environ.get("DB_HOST")
    DB_DATABASE = os.environ.get("DB_DATABASE")
    DB_USER = os.environ.get("DB_USER")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")

    AWS_SECRET = os.environ.get("AWS_SECRET")
    AWS_ACCESS = os.environ.get("AWS_ACCESS")
    AWS_REGION = os.environ.get("AWS_REGION")
