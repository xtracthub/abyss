import logging
import os
from configparser import ConfigParser
from typing import List, Dict

import psycopg2
import psycopg2.extras
from flask import Flask

ABYSS_STATUS = {
    "abyss_id": "TEXT PRIMARY KEY",
    "client_id": "TEXT",
    "crawl_status": "TEXT",
    "unpredicted": "INT",
    "unpredicted_predict": "INT",
    "unpredicted_schedule": "INT",
    "unpredicted_scheduled": "INT",
    "unpredicted_prefetching": "INT",
    "unpredicted_prefetched": "INT",
    "processing_headers": "INT",
    "predicted": "INT",
    "scheduled": "INT",
    "prefetching": "INT",
    "prefetched": "INT",
    "decompressing": "INT",
    "decompressed": "INT",
    "crawling": "INT",
    "consolidating": "INT",
    "succeeded": "INT",
    "failed": "INT"
}

ABYSS_TABLES = {"abyss_status": ABYSS_STATUS}
PROJECT_ROOT = os.path.realpath(os.path.dirname(__file__)) + "/"


def read_flask_db_config(app: Flask) -> dict:
    """Reads PostgreSQL credentials from a Flask app configuration.

    Parameters
    ----------
    app : Flask
        Flask app to read database configuration from.

    Returns
    -------
    credentials : dict
        Dictionary with credentials.
    """
    credentials = dict()

    credentials["host"] = app.config.get("DB_HOST")
    credentials["database"] = app.config.get("DB_DATABASE")
    credentials["user"] = app.config.get("DB_USER")
    credentials["password"] = app.config.get("DB_PASSWORD")

    return credentials


def read_db_config_file(config_file=os.path.join(PROJECT_ROOT,
                                                 "database.ini"),
                        section="postgresql") -> dict:
    """Reads PostgreSQL credentials from a .ini file.

    Parameters
    ----------
    config_file : str
        .ini config file to read database configuration from.
    section : str
        Section in .ini file to read credentials from.

    Returns
    -------
    credentials : dict
        Dictionary with credentials.
    """
    parser = ConfigParser()
    parser.read(config_file)

    credentials = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            credentials[param[0]] = param[1]

    else:
        raise Exception(
            f"Section {section} not found in the {config_file} file")

    return credentials


def create_connection(credentials: dict):
    """Creates a connection object to a PostgreSQL database.

    Parameters
    ----------
    credentials : Dict
        Database credentials.

    Returns
    -------
    conn
        Connection object to database.
    """
    conn = psycopg2.connect(**credentials)
    logging.info("Connection to database succeeded")

    return conn


def table_exists(conn, table_name: str) -> bool:
    """Checks whether a table exists in the database.

    Parameters
    ----------
    conn
        psycopg2 connection object.
    table_name : str
        Name of table to check exists.

    Returns
    -------
    bool
        Whether the table_name exists.
    """
    cur = conn.cursor()
    # cur.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE TABLE_NAME=%s)",
    #             (table_name,))
    cur.execute("SELECT to_regclass('public.abyss_status')")
    x = cur.fetchone()[0]
    print(x)

    return x


def build_tables(conn, tables: Dict[str, dict]) -> None:
    """Creates tables within a database.

    Parameters
    ----------
    conn
        psycopg2 connection object.
    tables : dict(dict)
        Dictionary mapping table name to dictionary containing table
        info. Table info dictionary contains table column name mapped to
        column type.

    Returns
    -------
    None
    """
    cur = conn.cursor()

    for table_name, table_info in tables.items():
        print(f"MAKING {table_name}")
        print(table_exists(conn, table_name))
        if not(table_exists(conn, table_name)):
            column_statements = []

            for column_name, column_type in table_info.items():
                column_statements.append(column_name + " " + column_type)

            table_statement = f"""CREATE TABLE {table_name} ({", ".join(column_statements)});"""
            print(table_statement)
            cur.execute(table_statement)

    cur.close()
    conn.commit()
    print("MADE DA TABLES")

    logging.info("Successfully created tables")


def create_table_entry(conn, table_name: str, **columns) -> None:
    """Creates a new entry within a table.

    Parameters
    ----------
    conn
        psycopg2 connection object.
    table_name : str
        Name of table to create an entry to. Must be a key in
        ABYSS_TABLES.
    columns
        Keyword arguments mapping columns in table_name to values. If
        no value for a column is passed then None is used.

    Returns
    -------
    None
    """
    if table_name not in ABYSS_TABLES.keys():
        raise ValueError(f"{table_name} not a valid table in Abyss database.")

    entry = []
    table = ABYSS_TABLES[table_name]

    assert set(list(columns.keys())) <= set(table), "Column does not exist in table"

    statement = f"""INSERT INTO {table_name} VALUES {"(" + ", ".join(["%s"] * len(table)) + ")"}"""

    for column in table:
        if column in columns:
            entry.append(columns[column])
        else:
            entry.append(None)

    entry = tuple(entry)

    cur = conn.cursor()
    cur.execute(statement, entry)
    conn.commit()
    logging.info(f"Successfully created entry to {table_name} table")


def update_table_entry(conn, table_name: str,
                       primary_key: dict, **columns) -> None:
    """Updates values within an existing table.

    Parameters
    ----------
    conn
        psycopg2 connection object.
    table_name : str
        Name of table to create an entry to. Must be a key in
        ABYSS_TABLES.
    primary_key : dict
        Dictionary mapping name of primary key of table_name to value.
    columns
        Keyword arguments mapping columns in table_name to values. If
        no value for a column is passed then None is used.
    Returns
    -------
    None
    """
    if table_name not in ABYSS_TABLES.keys():
        raise ValueError(f"{table_name} not a valid table in Abyss database.")

    table = ABYSS_TABLES[table_name]
    primary_key_name = list((primary_key.keys()))[0]
    primary_key_value = primary_key[primary_key_name]

    columns.update(primary_key)
    for column_name in columns.keys():
        if column_name not in table:
            raise ValueError(f"Column {column_name} does not exist in {table_name}.")

    if "PRIMARY KEY" not in table[primary_key_name]:
        raise ValueError(f"Column {primary_key_name} is not a primary key for {table_name}.")

    column_values = list(columns.values())
    column_names = list(columns.keys())

    column_string = " = %s,".join(column_names) + " = %s"
    column_values.append(primary_key_value)

    statement = f"""UPDATE {table_name}
                SET {column_string}
                WHERE {primary_key_name} = %s"""
    cur = conn.cursor()
    cur.execute(statement, tuple(column_values))
    conn.commit()


def select_all_rows(conn, table_name: str) -> List[Dict]:
    """Returns all rows from a table.

    Parameters
    ----------
    conn
        psycopg2 connection object.
    table_name : str
        Name of table to create an entry to. Must be a key in
        ABYSS_TABLES.

    Returns
    -------
    rows : list(dict)
        List of dictionaries containing column values.

    """
    if table_name not in ABYSS_TABLES.keys():
        raise ValueError(f"{table_name} not a valid table in Abyss database.")

    table = ABYSS_TABLES[table_name]
    rows = []

    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")

    results = cur.fetchall()

    for result in results:
        rows.append(dict(zip(table, result)))

    return rows


def select_by_column(conn, table_name: str, **columns) -> List[Dict]:
    """Queries table entries by column value.

    Parameters
    ----------
    conn
        psycopg2 connection object.
    table_name : str
        Name of table to create an entry to. Must be a key in
        ABYSS_TABLES.
    columns
        Keyword arguments mapping columns in table_name to values.

    Returns
    -------
    rows : list(dict)
        List of queried rows.
    """
    if table_name not in ABYSS_TABLES.keys():
        raise ValueError(f"{table_name} not a valid table in Abyss database.")

    table = ABYSS_TABLES[table_name]

    column_values = list(columns.values())
    column_names = list(columns.keys())

    for column_name in column_names:
        if column_name not in table:
            raise ValueError(f"Column {column_name} does not exist in {table_name}.")

    rows = []

    cur = conn.cursor()
    cur.execute(f"""SELECT * FROM {table_name} WHERE {"=%s AND ".join(columns) + "=%s"}""",
                column_values)

    results = cur.fetchall()

    for result in results:
        rows.append(dict(zip(table, result)))

    logging.info("Successfully queried {} columns".format(columns))

    return rows
