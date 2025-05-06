# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

from typing import Optional
import threading
import time

from dbmind.common.exceptions import ApiClientException
from dbmind.common.tsdb.dummy_client import DummyClient
from dbmind.common.tsdb.influxdb_client import InfluxdbClient
from dbmind.common.tsdb.prometheus_client import PrometheusClient
from dbmind.common.tsdb.tsdb_client import TsdbClient
from dbmind.common.types import SSLContext
from dbmind.common.utils import dbmind_assert, raise_fatal_and_exit
from dbmind.common.utils.checking import prepare_ip


class TsdbClientFactory(object):
    tsdb_name = host = port = dbname = None
    username = password = None
    ssl_context = None

    tsdb_client = None
    shared_lock = threading.Lock()

    @classmethod
    def get_tsdb_client(cls) -> Optional[TsdbClient]:
        if cls.tsdb_client is not None:
            return cls.tsdb_client
        with cls.shared_lock:
            if cls.tsdb_client is None:
                cls._init_client()
        return cls.tsdb_client

    @classmethod
    def set_client_info(cls, tsdb_name, host, port, username=None, password=None,
                        ssl_certfile=None, ssl_keyfile=None,
                        ssl_keyfile_password=None, ssl_ca_file=None, dbname=None):
        cls.tsdb_name = tsdb_name
        cls.host = host
        cls.port = port
        cls.username = username
        cls.password = password
        cls.dbname = dbname

        cls.ssl_context = SSLContext(
            ssl_certfile,
            ssl_keyfile,
            ssl_keyfile_password,
            ssl_ca_file
        )
        return cls

    @classmethod
    def _init_client(cls):
        if cls.tsdb_name != 'ignore':
            dbmind_assert(cls.tsdb_name and cls.host and cls.port)

        if cls.ssl_context:
            url = f"https://{prepare_ip(cls.host)}:{cls.port}"
        else:
            url = f"http://{prepare_ip(cls.host)}:{cls.port}"

        if cls.tsdb_name == 'prometheus':
            client = PrometheusClient(url=url, username=cls.username, password=cls.password,
                                      ssl_context=cls.ssl_context)
            if not client.check_connection():
                raise ApiClientException("Failed to connect TSDB url: %s" % url)
            cls.tsdb_client = client
        elif cls.tsdb_name == 'influxdb':
            client = InfluxdbClient(url=url, username=cls.username, password=cls.password,
                                    ssl_context=cls.ssl_context, dbname=cls.dbname)
            if not client.check_connection():
                raise ApiClientException("Failed to connect TSDB url: %s" % url)
            cls.tsdb_client = client
        elif cls.tsdb_name == 'ignore':
            client = DummyClient()
            cls.tsdb_client = client

        if cls.tsdb_client is None:
            raise ApiClientException("Failed to init TSDB client, please check config file.")

        if (cls.tsdb_client.timestamp() > 0 and
                abs(cls.tsdb_client.timestamp() - time.time() * 1000) > 60 * 1000):  # threshold is 1 minute.
            raise_fatal_and_exit('Found clock drift between TSDB client and server, '
                                 'please check and synchronize system clocks.')
