import pyodbc
import configparser
import psycopg2
import constants
import sys
import json


def get_sql_connection():
    """
    This method simply returns the sql connection based on the DB type and the connection settings
    defined in the db.conf
    :return: connection
    """

    # Reading the Database configurations
    database = json.load(open(sys.argv[1]))["psql_connect"]
    return psycopg2.connect(database=json.load(open(sys.argv[1]))["dataset"], user=database["pg_user"], password=database["pg_password"],
                          host=database["pg_ip"], port=database["pg_port"])


def close_sql_connection(connection):
    """
    Take care of the closing process of the SQL connection
    :param connection: sql_connection
    :return: operation status
    """
    return connection.close()
