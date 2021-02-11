import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    GLOBUS_KEY = os.environ.get("GLOBUS_KEY")
    GLOBUS_CLIENT = os.environ.get("GLOBUS_CLIENT")
