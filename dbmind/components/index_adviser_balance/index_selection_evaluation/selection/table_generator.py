import logging
import os
import platform
import re
import subprocess

from .utils import b_to_mb
from .workload import Column, Table


class TableGenerator:

    def __init__(self,
                 benchmark_name,
                 scale_factor,
                 database_connector,
                 dbname,
                 explicit_database_name=None):
        self.scale_factor = scale_factor
        self.benchmark_name = benchmark_name
        self.db_connector = database_connector
        self.explicit_database_name = explicit_database_name
        self.dbname = dbname
        self.database_names = self.db_connector.database_names()
        self.tables = []
        self.columns = []
        self.types = set()
        self._prepare()
        self._read_column_names()



    def database_name(self):

        return self.dbname

    def _read_column_names(self):
        # Read table and column names from 'create table' statements
        filename = self.directory + "/" + self.create_table_statements_file
        with open(filename, "r") as file:
            data = file.read().lower()
        create_tables = data.split("create table ")[1:]
        for create_table in create_tables:
            splitted = create_table.split("(", 1)
            table = Table(splitted[0].strip())
            self.tables.append(table)
            # TODO regex split? ,[whitespace]\n
            for column in splitted[1].split(",\n"):
                name = column.lstrip().split(" ", 1)[0]
                if name == "primary":
                    continue
                column_object = Column(name)
                table.add_column(column_object)
                self.columns.append(column_object)
                self.types.add(re.split(r"[ ]+", column.lstrip())[1])

    def _prepare(self):
        if self.benchmark_name == "tpch":
            self.directory = "./index_selection_evaluation/tpch-kit/dbgen"
            self.create_table_statements_file = "dss.ddl"
        elif self.benchmark_name == "tpcds":
            self.directory = "./index_selection_evaluation/tpcds-kit/tools"
            self.create_table_statements_file = "tpcds.sql"
        else:
            raise NotImplementedError("only tpch/ds implemented.")
