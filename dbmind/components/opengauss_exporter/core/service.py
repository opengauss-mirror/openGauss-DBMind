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
import os
import time
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from types import SimpleNamespace

from prometheus_client import (
    Gauge, Summary, Histogram, Info, Enum
)
from prometheus_client.exposition import generate_latest
from prometheus_client.registry import CollectorRegistry

from dbmind import constants
from dbmind.common import ha
from dbmind.common.opengauss_driver import DriverBundle
from dbmind.common.parser.sql_parsing import standardize_sql
from dbmind.common.utils import dbmind_assert
from . import main

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

driver = SimpleNamespace()

# yaml macros
scrape_interval_seconds = 0
long_transaction_threshold_seconds = 3600
busy_transaction_threshold_count = 100

DEFAULT_COVERAGE_WINDOW = 300  # seconds

catalogs = {
    "Centralized": {
        "session_memory_detail": "gs_session_memory_detail",
        "thread_memory_context": "gs_thread_memory_context",
        "total_memory_detail": "gs_total_memory_detail",
        "shared_memory_detail": "gs_shared_memory_detail",
        "redo_stat": "gs_redo_stat",
        "excluded_usernames": "('rdsAdmin','rdsMetric','rdsBackup','rdsRepl','dbmind_monitor','dbmind_monitor_agent')"
    },
    "Distribute": {
        "session_memory_detail": "pv_session_memory_detail",
        "thread_memory_context": "pv_thread_memory_context",
        "total_memory_detail": "pv_total_memory_detail",
        "shared_memory_detail": "pg_shared_memory_detail",
        "redo_stat": "pv_redo_stat",
        "excluded_usernames": "('rdsAdmin','rdsMetric','rdsBackup','rdsRepl','dbmind_monitor','dbmind_monitor_agent')"
    }
}

_thread_pool_executor = SimpleNamespace()
REGISTRY = CollectorRegistry()

_use_cache = True
global_labels = {FROM_INSTANCE_KEY: ''}

_dbrole = DBROLE_UNSET  # default
_dbversion = '9.2.24'
_deployment = None

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

    def __init__(self, sql, timeout=1, ttl=0):
        self.sql = sql
        self.timeout = timeout
        self.ttl = ttl  # cache_seconds for PG exporter
        self._cache = None
        self._last_scrape_timestamp = int(time.time() * 1000) - 15000  # Default value is 15 seconds ago.

    def fetch(self, alternative_timeout, force_connection_db=None):
        current_timestamp = int(time.time() * 1000)
        if (
            self.ttl and self._cache and
            current_timestamp - self._last_scrape_timestamp < self.ttl * 1000
        ):
            return self._cache

        macro_mapper = {
            'last_scrape_timestamp': self._last_scrape_timestamp,
            'scrape_interval': (
                scrape_interval_seconds * 1000 or
                current_timestamp - self._last_scrape_timestamp
            ),
            'scrape_interval_seconds': (
                scrape_interval_seconds or
                int((current_timestamp - self._last_scrape_timestamp) / 1000)
            ),
            'long_transaction_threshold_seconds': long_transaction_threshold_seconds,
            'busy_transaction_threshold_count': busy_transaction_threshold_count
        }
        if isinstance(_deployment, str) and "Centralized" in _deployment:
            macro_mapper.update(catalogs.get("Centralized", {}))
        elif isinstance(_deployment, str) and "Distribute" in _deployment:
            macro_mapper.update(catalogs.get("Distribute", {}))
        else:
            logging.warning("Deployment illegal: %s", _deployment)

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

    def set_value(self, value, labels):
        metric_family = self.value.labels(**labels)
        value = cast_to_numeric(value)
        # Different usages (Prometheus data type) have different setting methods.
        # Thus, we have to select to different if-branches according to metric's usage.
        if self.usage == 'COUNTER':
            metric_family.set(value)
        elif self.usage == 'GAUGE':
            metric_family.set(value)
        elif self.usage == 'SUMMARY':
            metric_family.observe(value)
        elif self.usage == 'HISTOGRAM':
            metric_family.observe(value)
        else:
            logging.error('Not supported metric %s due to usage %s.', self.name, self.usage)


def process_particular_field(field_name, field_value):
    """Transform a particular field's value."""
    if field_name == 'query':
        return standardize_sql(field_value)
    return field_value


class QueryInstance:
    def __init__(self, d):
        self.name = d['name']
        self.desc = d.get('desc', '')
        self.query = None
        self.metrics = list()
        self.labels = list()
        self.status = d.get('status', STATUS_ENABLE) == STATUS_ENABLE
        self.ttl = d.get('ttl', 0)
        self.timeout = d.get('timeout', 0)
        self.dbrole = d.get('dbRole') or DBROLE_UNSET  # primary, standby, ...
        self.version = d.get('version', DEFAULT_VERSION)
        self.null_coverage_window = d.get('null_coverage_window', DEFAULT_COVERAGE_WINDOW)
        self.metric_clock = dict()

        # Compatible with PG-exporter
        dbrole_condition = self.dbrole in (DBROLE_UNSET, _dbrole)
        if self.status and dbrole_condition and is_valid_version(self.version):
            self.query = Query(d['query'], timeout=self.timeout, ttl=self.ttl)
            logging.info('Record the query %s (status: %s, dbRole: %s, version: %s).',
                         self.name, self.status, self.dbrole, self.version)
        else:
            logging.info('Skip the query %s (status: %s, dbRole: %s, version: %s).',
                         self.name, self.status, self.dbrole, self.version)

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
            registry.register(metric.activate(self.labels))

    def force_query_into_particular_db(self, db_name):
        self._forcing_db = db_name

    def update(self):
        if isinstance(driver, DriverBundle):
            global_labels[FROM_INSTANCE_KEY] = driver.address

        if self.query is None:
            return

        # Force the query into connecting to the specific database
        # rather than the default database, if needed.
        try:
            rows = self.query.fetch(self.timeout, self._forcing_db)
        except Exception as e:
            logging.exception(e)
            logging.info("Error SQL statement is '%s'.", self.query.sql)
            rows = []
        else:
            if not rows:
                logging.warning("Fetched nothing for metric '%s'.", self.name)
                rows = []

        scraped_label_values = dict()
        # Update for all metrics in current query instance.
        for row in rows:
            # `global_labels` is the essential labels for each metric family.
            labels = {}
            for field_name in self.labels:
                field_value = str(row.get(field_name, global_labels.get(field_name)))
                field_value = process_particular_field(field_name, field_value)
                labels[field_name] = field_value

            label_values = tuple(str(labels[k]) for k in self.labels)
            scraped_label_values[label_values] = (labels, row)
            self.metric_clock[label_values] = time.monotonic() + self.null_coverage_window

        for metric in self.metrics:
            # Scraped label_values - directly set
            for label_values, (labels, row) in scraped_label_values.items():
                value = row.get(metric.name)
                # None is equivalent to NaN instead of zero.
                if value is None:
                    logging.warning('Not found field %s in the %s.', metric.name, self.name)

                metric.set_value(value, labels)

            for label_values in metric.value._metrics.keys() - scraped_label_values.keys():
                if metric.prefix in ["pg_long_transaction"]:
                    metric.value.remove(*label_values)
                    if label_values in self.metric_clock:
                        self.metric_clock.pop(label_values)
                    continue

                # Remove the expired metric
                if self.metric_clock.get(label_values, 0) < time.monotonic():
                    logging.info("The metric: %s_%s of %s was discarded.",
                                 metric.prefix, metric.name, label_values)
                    metric.value.remove(*label_values)
                    if label_values in self.metric_clock:
                        self.metric_clock.pop(label_values)


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
    global _dbrole, _deployment
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
    _deployment = driver.deployment
    # Append extra labels, including essential labels (e.g., from_server)
    # and constant labels from user's configurations.
    global_labels[FROM_INSTANCE_KEY] = driver.address
    global_labels.update(constant_labels)
    logging.info('Monitoring %s, use cache: %s, extra labels: %s.',
                 global_labels[FROM_INSTANCE_KEY], _use_cache, global_labels)


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
        if driver.multi_connection and not raw_query_instance.get("for_dist_dn"):
            continue

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
    REGISTRY.register(exporter_fixed_info)


def update_exporter_fixed_info(k, v):
    EXPORTER_FIXED_INFO[k] = v


def query_all_metrics():
    # refresh fixed info below
    try:
        is_primary = not driver.is_standby()
    except (ConnectionError, IndexError):
        is_primary = None

    update_exporter_fixed_info('monitoring', driver.address)
    update_exporter_fixed_info('primary', is_primary)
    # Notice: have to get the private variable
    exporter_fixed_info = getattr(REGISTRY, '_names_to_collectors')['opengauss_exporter_fixed_info']
    exporter_fixed_info.clear()
    exporter_fixed_info.labels(**EXPORTER_FIXED_INFO).set(1)

    # Only when all tasks in the thread pool have been executed will the next task be allocated.
    if _thread_pool_executor._work_queue.qsize() > 0:
        return generate_latest(REGISTRY)

    futures = []
    for instance in query_instances:
        futures.append(_thread_pool_executor.submit(instance.update))

    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            logging.exception(e)

    return generate_latest(REGISTRY)


def check_status_opengauss_exporter(cmd):
    cur_path = os.path.realpath(os.path.dirname(__file__))
    proj_path = cur_path[:cur_path.rfind('dbmind')]
    log_path = main.exporter_info_dict['logfile']
    if main.exporter_info_dict['constant_labels_instance']:
        pid_file = os.path.join(
            proj_path,
            f"opengauss_exporter_{main.exporter_info_dict['constant_labels_instance']}.pid"
        )
    else:
        pid_file = os.path.join(proj_path, 'opengauss_exporter.pid')
    status_info = ha.check_status_impl(log_path, pid_file, 'opengauss_exporter', (driver,))
    ha.record_interface_info('check_status', status_info)
    return status_info


def repair_interface_opengauss_exporter(cmd):
    cur_path = os.path.realpath(os.path.dirname(__file__))
    proj_path = cur_path[:cur_path.rfind('dbmind')]
    log_path = main.exporter_info_dict['logfile']
    if main.exporter_info_dict['constant_labels_instance']:
        pid_file = os.path.join(
            proj_path,
            f"opengauss_exporter_{main.exporter_info_dict['constant_labels_instance']}.pid"
        )
    else:
        pid_file = os.path.join(proj_path, 'opengauss_exporter.pid')
    repair_info = ha.repair_interface_impl(log_path, pid_file, 'opengauss_exporter', (driver,))
    ha.record_interface_info('repair', repair_info)
    return repair_info
