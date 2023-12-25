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
import logging
import time
import os
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime

from prometheus_client import (
    Gauge, Summary, Histogram, Info, Enum
)
from prometheus_client.exposition import generate_latest
from prometheus_client.registry import CollectorRegistry

from dbmind.common.utils import dbmind_assert
from dbmind import constants
from dbmind.common.parser.sql_parsing import standardize_sql
from dbmind.common.opengauss_driver import DriverBundle

STATUS_ENABLE = "enable"
STATUS_DISABLE = "disable"
DEFAULT_VERSION = ">=0.0.0"
DBROLE_PRIMARY = 'primary'
DBROLE_STANDBY = 'standby'
DBROLE_UNSET = 'unset'

PROMETHEUS_TYPES = {
    # Indeed, COUNTER should use the type `Counter` rather than `Gauge`,
    # but PG-exporter and openGauss-exporter (golang version)
    # are all using ConstValue (i.e., the same action as Gauge),
    # so we have to inherit the usage.
    'COUNTER': Gauge, 'GAUGE': Gauge, 'SUMMARY': Summary,
    'HISTOGRAM': Histogram, 'INFO': Info, 'ENUM': Enum
}

PROMETHEUS_LABEL = 'LABEL'
PROMETHEUS_DISCARD = 'DISCARD'
FROM_INSTANCE_KEY = 'from_instance'

EXPORTER_FIXED_INFO = {
    # fixed name below
    # Don't change the following fixed key name casually because
    # some scenarios may depend on them.
    'version': constants.__version__,
    'updated': os.stat(
        os.path.realpath(
            os.path.join(os.path.dirname(__file__), '../yamls')
        )
    ).st_ctime,  # the last updated timestamp for yaml files
    'url': None,  # exporter url
    'primary': None,  # true means primary, false means standby
    'rpc': None,  # support rpc, true or false
    'dbname': None,  # which database to monitor
    'monitoring': None  # what address is the database instance
}

driver = None

# yaml macros
scrape_interval_seconds = 0

_thread_pool_executor = None
REGISTRY = CollectorRegistry()

_use_cache = True
global_labels = {FROM_INSTANCE_KEY: ''}

_dbrole = DBROLE_UNSET  # default
_dbversion = '9.2.24'

query_instances = list()


def is_valid_version(version):
    """Not implemented yet."""
    return True


def cast_to_numeric(v):
    if v is None:
        return float('nan')
    elif isinstance(v, datetime):
        return int(v.timestamp() * 1000)
    else:
        return float(v)


class Query:
    """Maybe only a SQL statement for PG exporter."""

    def __init__(self, item):
        self.name = item.get('name')
        self.sql = item['sql']
        self.version = item.get('version', DEFAULT_VERSION)
        self.timeout = item.get('timeout')
        self.ttl = item.get('ttl', 0)  # cache_seconds for PG exporter
        self.status = item.get('status', STATUS_ENABLE) == STATUS_ENABLE  # enable or disable
        self.dbrole = item.get('dbRole') or DBROLE_UNSET  # primary, standby, ...

        self._cache = None
        self._last_scrape_timestamp = int(time.time() * 1000) - 15000  # Default value is 15 seconds ago.

    def fetch(self, alternative_timeout, force_connection_db=None):
        current_timestamp = int(time.time() * 1000)
        macro_mapper = {
            'last_scrape_timestamp': self._last_scrape_timestamp,
            'scrape_interval': scrape_interval_seconds * 1000 or current_timestamp - self._last_scrape_timestamp,
            'scrape_interval_seconds': scrape_interval_seconds or int(
                (current_timestamp - self._last_scrape_timestamp) / 1000)
        }

        if self._cache and (current_timestamp - self._last_scrape_timestamp) < (self.ttl * 1000):
            return self._cache

        # Refresh cache:
        # If the query gives explict timeout, then use it,
        # otherwise use passed `alternative_timeout`.
        formatted = self.sql.format_map(macro_mapper)  # If the SQL has the placeholder, render it.
        logging.debug('Query the SQL statement: %s.', formatted)
        self._cache = driver.query(formatted,
                                   self.timeout or alternative_timeout,
                                   force_connection_db)
        self._last_scrape_timestamp = current_timestamp
        return self._cache


class Metric:
    """Metric family structure:
    Only parsing the metric dict and
    lazy loading the Prometheus metric object."""

    def __init__(self, item):
        self.name = item['name']
        self.desc = item.get('description', '')
        self.usage = item['usage'].upper()
        self.value = None
        self.prefix = ''
        self.is_label = False
        self.is_valid = False

        if self.usage in PROMETHEUS_TYPES:
            """Supported metric type."""
            self.is_valid = True
        elif self.usage == PROMETHEUS_LABEL:
            """Use the `is_label` field to mark this metric as a label."""
            self.is_label = True
            self.is_valid = True
        elif self.usage == PROMETHEUS_DISCARD:
            """DISCARD means do nothing."""
            self.is_valid = False
        else:
            raise ValueError('Not supported usage %s.' % self.usage)

    def activate(self, labels=()):
        """Instantiate specific Prometheus metric objects."""
        dbmind_assert(not self.is_label and self.prefix)

        self.value = (PROMETHEUS_TYPES[self.usage])(
            # Prefix query instance name to the specific metric.
            '%s_%s' % (self.prefix, self.name), self.desc, labels
        )
        return self.value


def process_particular_field(field_name, field_value):
    """Transform a particular field's value."""
    if field_name == 'query':
        return standardize_sql(field_value)
    return field_value


class QueryInstance:
    def __init__(self, d):
        self.name = d['name']
        self.desc = d.get('desc', '')
        self.queries = list()
        self.metrics = list()
        self.labels = list()
        self.status = d.get('status', STATUS_ENABLE) == STATUS_ENABLE
        self.ttl = d.get('ttl', 0)
        self.timeout = d.get('timeout', 0)

        # Compatible with PG-exporter format,
        # convert the query field into a list.
        if isinstance(d['query'], str):
            d['query'] = [
                {'name': self.name, 'sql': d['query'], 'ttl': self.ttl, 'timeout': self.timeout}
            ]

        dbmind_assert(isinstance(d['query'], list))
        for q in d['query']:
            # Compatible with PG-exporter
            query = Query(q)
            dbrole_condition = query.dbrole in (DBROLE_UNSET, _dbrole)
            if query.status and dbrole_condition and is_valid_version(query.version):
                self.queries.append(query)
                logging.info('Record the query %s (status: %s, dbRole: %s, version: %s).' % (
                    query.name, query.status, query.dbrole, query.version))
            else:
                logging.info('Skip the query %s (status: %s, dbRole: %s, version: %s).' % (
                    query.name, query.status, query.dbrole, query.version))

        for m in d['metrics']:
            # Compatible with PG-exporter
            if len(m) == len({'metric_name': {'usage': '?', 'description': '?'}}):
                # Covert to the openGauss-exporter format.
                # The following is a demo for metric structure in the openGauss-exporter:
                # {'name': 'metric_name', 'usage': '?', 'description': '?'}
                name, value = next(iter(m.items()))
                m = {'name': name}
                m.update(value)

            # Parse dict structure to a Metric object, then we can
            # use this object's fields directly.
            metric = Metric(m)
            if not metric.is_valid:
                continue
            if not metric.is_label:
                metric.prefix = self.name
                self.metrics.append(metric)
            else:
                self.labels.append(metric.name)

        # `global_labels` is required and must be added anytime.
        self.labels.extend(global_labels.keys())
        self._forcing_db = None

    def register(self, registry):
        for metric in self.metrics:
            registry.register(
                metric.activate(self.labels)
            )

    def force_query_into_particular_db(self, db_name):
        self._forcing_db = db_name

    def update(self):
        # Clear old metric's value and its labels.
        for metric in self.metrics:
            metric.value.clear()

        for query in self.queries:
            # Force the query into connecting to the specific database
            # rather than the default database, if needed.
            try:
                rows = query.fetch(self.timeout, self._forcing_db)
            except Exception as e:
                logging.exception(e)
                logging.info("Error SQL statement is '%s'.", query.sql)
                continue
            else:
                if len(rows) == 0:
                    logging.warning("Fetched nothing for metric '%s'." % query.name)
                    continue

            # Update for all metrics in current query instance.
            for row in rows:
                # `global_labels` is the essential labels for each metric family.
                labels = {}
                for field_name in self.labels:
                    field_value = str(row.get(field_name, global_labels.get(field_name)))
                    field_value = process_particular_field(field_name, field_value)
                    labels[field_name] = field_value

                for metric in self.metrics:
                    metric_family = metric.value.labels(**labels)
                    value = row.get(metric.name)
                    # None is equivalent to NaN instead of zero.
                    if value is None:
                        logging.warning(
                            'Not found field %s in the %s.', metric.name, self.name
                        )

                    value = cast_to_numeric(value)
                    # Different usages (Prometheus data type) have different setting methods.
                    # Thus, we have to select to different if-branches according to metric's usage.
                    if metric.usage == 'COUNTER':
                        metric_family.set(value)
                    elif metric.usage == 'GAUGE':
                        metric_family.set(value)
                    elif metric.usage == 'SUMMARY':
                        metric_family.observe(value)
                    elif metric.usage == 'HISTOGRAM':
                        metric_family.observe(value)
                    else:
                        logging.error(
                            'Not supported metric %s due to usage %s.' % (metric.name, metric.usage)
                        )


def config_collecting_params(
        url,
        include_databases,
        exclude_databases,
        parallel,
        connection_pool_size,
        disable_cache,
        constant_labels,
        **kwargs
):
    global _use_cache, _thread_pool_executor
    global _dbrole
    global driver, scrape_interval_seconds

    # Set global yaml config macros.
    scrape_interval_seconds = kwargs.get('scrape_interval_seconds', 0)

    driver = DriverBundle(
        url, include_databases, exclude_databases,
        each_db_max_connections=connection_pool_size
    )
    _thread_pool_executor = ThreadPoolExecutor(max_workers=parallel)
    _use_cache = not disable_cache
    _dbrole = DBROLE_STANDBY if driver.is_standby() else DBROLE_PRIMARY
    # Append extra labels, including essential labels (e.g., from_server)
    # and constant labels from user's configurations.
    global_labels[FROM_INSTANCE_KEY] = driver.address
    global_labels.update(constant_labels)
    logging.info(
        'Monitoring %s, use cache: %s, extra labels: %s.',
        global_labels[FROM_INSTANCE_KEY], _use_cache, global_labels
    )


def register_metrics(parsed_yml, force_connection_db=None):
    """Some metrics need to be queried on the specific database
    (e.g., tables or views under the dbe_perf schema need
    to query on the `postgres` database).
    Therefore, we cannot specify that all metrics are collected
    through the default database,
    and this is the purpose of the parameter `force_connection_db`.
    """
    dbmind_assert(isinstance(parsed_yml, dict))

    for name, raw_query_instance in parsed_yml.items():
        dbmind_assert(isinstance(raw_query_instance, dict))

        raw_query_instance.setdefault('name', name)
        instance = QueryInstance(raw_query_instance)
        instance.force_query_into_particular_db(force_connection_db)
        instance.register(REGISTRY)
        query_instances.append(instance)


def register_exporter_fixed_info():
    # register fixed metric, which is similar to
    # node_exporter_build_info etc.
    exporter_fixed_info = Gauge(
        name='opengauss_exporter_fixed_info',
        documentation='build and monitoring info',
        labelnames=EXPORTER_FIXED_INFO.keys()
    )
    REGISTRY.register(
        exporter_fixed_info
    )


def update_exporter_fixed_info(k, v):
    EXPORTER_FIXED_INFO[k] = v


def query_all_metrics():
    futures = []
    if _thread_pool_executor._work_queue.qsize() == 0:
        for instance in query_instances:
            futures.append(_thread_pool_executor.submit(instance.update))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.exception(e)

        # refresh fixed info below
        try:
            is_primary = not driver.is_standby()
        except IndexError:
            is_primary = None
        update_exporter_fixed_info('primary', is_primary)
        # Notice: have to get the private variable
        exporter_fixed_info = getattr(REGISTRY, '_names_to_collectors')[
            'opengauss_exporter_fixed_info'
        ]
        exporter_fixed_info.clear()
        exporter_fixed_info.labels(**EXPORTER_FIXED_INFO).set(1)

    return generate_latest(REGISTRY)
