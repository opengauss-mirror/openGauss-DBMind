# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
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
