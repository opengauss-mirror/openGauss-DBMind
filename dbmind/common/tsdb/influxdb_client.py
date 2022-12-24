# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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

from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse

from dbmind.common.http.requests_utils import create_requests_session
from dbmind.common.utils import cached_property
from dbmind.constants import DISTINGUISHING_INSTANCE_LABEL

from .tsdb_client import TsdbClient, cast_duration_to_seconds
from ..exceptions import ApiClientException
from ..types import Sequence
from ..types.ssl import SSLContext


def _influxql_generator(
        metric_name: str,
        node_name: str,
        start_ts_in_s: float = None,
        end_ts_in_s: float = None,
        label_config: dict = None,
        labels_like: dict = None,
):
    r"""
    The InfluxDB API only accepts query in influxql in params['q']. This method is to get the well-formed
    influxql with given metric_name, node_name, start timestamp in seconds, end timestamp in seconds,
    label configuration and regex labels.
    :param metric_name: (str) The name of the metric.
    :param node_name: (str) The exact node_name key of the metric_name.
    :param start_ts_in_s:  (float) A timestamp number in seconds that specifies the metric range start time.
    :param end_ts_in_s: (float) A timestamp number in seconds that specifies the metric range end time.
    :param label_config: (dict) A dictionary contains the exact filting information
    :param labels_like: (dict) A dictionary contains the patial filting information in form of regex.
    :return: (list) A list of filted sequences of the specified metric_name between the given time range.
    """

    def time_filter_generator(start_ts_in_ms, end_ts_in_ms):
        time_filter = list()
        if start_ts_in_ms:
            time_filter.append(f"time >= {start_ts_in_ms}ms")
        if end_ts_in_ms:
            time_filter.append(f"time <= {end_ts_in_ms}ms")
        return time_filter

    def condition_filter_generator(label_config, labels_like):
        filters = list()
        if label_config:
            for k, v in label_config.items():
                if k.endswith("!"):
                    filters.append(f"{k} !~ /^{v}$/")
                else:
                    filters.append(f"{k} =~ /^{v}$/")

        if labels_like:
            for k, v in labels_like.items():
                if k.endswith("!"):
                    filters.append(f"{k} !~ /{v}/")
                else:
                    filters.append(f"{k} =~ /{v}/")

        return filters

    filters = condition_filter_generator(label_config, labels_like)
    start_ts_in_ms = int(start_ts_in_s * 1000) if start_ts_in_s else None
    end_ts_in_ms = int(end_ts_in_s * 1000) if end_ts_in_s else None
    filters += time_filter_generator(start_ts_in_ms, end_ts_in_ms)
    filters = "WHERE " + " AND ".join(filters) if filters else ""
    group_by = f"GROUP BY {node_name}"
    influxql = f"SELECT value, dbname, {node_name} FROM {metric_name} {filters} {group_by}"

    return influxql


# Standardized the format of return value.
def _standardize(data):
    results_dict = defaultdict(list)
    for d in data["results"]:
        if "error" in d:
            raise ValueError(d.get("error"))

        if "series" not in d:
            return list()

        for series in d["series"]:
            for timestamp, value, dbname, node_name in series["values"]:
                metric_name = series["name"]
                results_dict[(metric_name, dbname, node_name)].append((timestamp, value))

    results = list()
    for (metric_name, dbname, node), time_value_list in results_dict.items():
        results.append(
            Sequence(
                timestamps=tuple(int(t_v[0]) for t_v in time_value_list),
                values=tuple(float(t_v[1]) for t_v in time_value_list),
                name=metric_name,
                labels={"datname": dbname, DISTINGUISHING_INSTANCE_LABEL: node},
            )
        )

    return results


class InfluxdbClient(TsdbClient):
    def __init__(
            self,
            url: str,
            username: str = None,
            password: str = None,
            ssl_context: SSLContext = None,
            headers: dict = None,
            dbname: str = None,
    ):
        """Functions as a Constructor for the class InfluxDB Connect."""
        if url is None:
            raise TypeError("missing url")

        self.headers = headers
        self.url = url
        self.influxdb_host = urlparse(self.url).netloc
        self._all_metrics = None

        self.dbname = dbname if dbname else self._dbname

        self._session_args = (username, password, ssl_context)

    def _get(self, url, **kwargs):
        with create_requests_session(*self._session_args) as session:
            return session.get(url=url, **kwargs)

    def check_connection(self, params: dict = None) -> bool:
        """
        Check InfluxDB connection.
        :param params: (dict) Optional dictionary containing parameters to be
            sent along with the API request.
        :returns: (bool) True if the endpoint can be reached, False if cannot be reached.
        """
        response = self._get(
            f"{self.url}/ping",
            headers=self.headers,
            params=params
        )
        return response.ok

    def get_current_metric_value(
            self, metric_name: str, label_config: dict = None, params: dict = None
    ):
        r"""
        Get the current metric target for the specified metric and label configuration.
        :param metric_name: (str) The name of the metric
        :param label_config: (dict) A dictionary that specifies metric labels and their
            values
        :param params: (dict) Optional dictionary containing GET parameters to be sent
            along with the API request, such as "epoch", "db"
        :returns: (list) A list of current metric values for the specified metric
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (ApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        labels_like = params.pop("labels_like") if "labels_like" in params else {}
        current_ts_in_s = params.pop("time") if "time" in params else datetime.timestamp(datetime.now())
        node_name = self._get_node_name(metric_name)
        influxql = _influxql_generator(
            metric_name,
            node_name,
            start_ts_in_s=None,
            end_ts_in_s=current_ts_in_s,
            label_config=label_config,
            labels_like=labels_like
        )
        influxql = influxql.replace("SELECT value", "SELECT last(value)")
        # using the query API to get raw data
        response = self._get(
            f"{self.url}/query",
            params={**params, **{"q": influxql, "epoch": "ms", "db": self.dbname}},
            headers=self.headers,
        )
        if response.status_code == 200:
            data = response.json()
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )

        return _standardize(data)

    def get_metric_range_data(
            self,
            metric_name: str,
            label_config: dict = None,
            start_time: datetime = (datetime.now() - timedelta(minutes=30)),
            end_time: datetime = datetime.now(),
            chunk_size: timedelta = None,
            step: str = None,
            params: dict = None
    ):
        r"""
        Get the current metric target for the specified metric and label configuration.
        :param metric_name: (str) The name of the metric.
        :param label_config: (dict) A dictionary specifying metric labels and their
            values.
        :param start_time:  (datetime) A datetime object that specifies the metric range start time.
        :param end_time: (datetime) A datetime object that specifies the metric range end time.
        :param chunk_size: (timedelta) Duration of metric data downloaded in one request. For
            example, setting it to timedelta(hours=3) will download 3 hours worth of data in each
            request made to the InfluxDB host
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :return: (list) A list of metric data for the specified metric in the given time
            range
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (ApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        labels_like = params.pop("labels_like") if "labels_like" in params else {}
        node_name = self._get_node_name(metric_name)
        start_ts_in_s = datetime.timestamp(start_time)
        end_ts_in_s = datetime.timestamp(end_time)
        influxql = _influxql_generator(
            metric_name,
            node_name,
            start_ts_in_s=start_ts_in_s,
            end_ts_in_s=end_ts_in_s,
            label_config=label_config,
            labels_like=labels_like
        )
        response = self._get(
            f"{self.url}/query",
            params={**params, **{"q": influxql, "epoch": "ms", "db": self.dbname}},
            headers=self.headers,
        )
        if response.status_code == 200:
            data = response.json()
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )

        return _standardize(data)

    def custom_query(self, query: str, timeout=None, params: dict = None):
        """
        Send an influxql to a InfluxDB Host.
        This method takes as input a string which will be sent as a query to
        the specified InfluxDB Host. This query is a influxql query.
        :param timeout: wait for query
        :param query: (str) This is a influxql query
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "epoch", "db"
        :returns: (list) A list of metric data received in response of the query sent
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (ApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        query = str(query)
        # using the query API to get raw data
        response = self._get(
            f"{self.url}/query",
            params={**params, **{"q": query, "epoch": "ms", "db": self.dbname}},
            headers=self.headers,
            timeout=timeout
        )
        if response.status_code == 200:
            data = response.json()
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )

        return _standardize(data)

    def _get_node_name(self, metric_name):
        response = self._get(
            f"{self.url}/query",
            params={
                "q": f"select value, nodename, node_name from {metric_name} limit 1",
                "db": self.dbname
            },
        ).json()
        _, _, nodename, node_name = response["results"][0]["series"][0]["values"][0]
        if nodename:
            return "nodename"
        elif node_name:
            return "node_name"

    def timestamp(self):
        influxql = "SHOW DIAGNOSTICS"
        response = self._get(
            f"{self.url}/query",
            params={"q": influxql, "epoch": "ms", "db": self.dbname},
        )
        if response.status_code == 200 and "series" in response.json()['results'][0]:
            data = response.json()
            for d in data["results"][0]["series"]:
                if d.get("name") == "system":
                    time_str = d.get("values")[0][d.get("columns").index("currentTime")]
                    ts = datetime.timestamp(datetime.strptime(time_str[:19], "%Y-%m-%dT%H:%M:%S"))
                    return (ts + 8 * 60 * 60) * 1000
        return 0

    @cached_property
    def scrape_interval(self):
        influxql = "SHOW DIAGNOSTICS"
        response = self._get(
            f"{self.url}/query",
            params={"q": influxql, "epoch": "ms", "db": self.dbname},
        )
        if response.status_code == 200 and "series" in response.json()['results'][0]:
            data = response.json()
            for d in data["results"][0]["series"]:
                if d.get("name") == "config-data":
                    interval = d.get("values")[0][d.get("columns").index("cache-snapshot-write-cold-duration")]
                    return cast_duration_to_seconds(interval)
        return None

    @cached_property
    def all_metrics(self):
        influxql = "SHOW TAG KEYS"
        response = self._get(
            f"{self.url}/query",
            params={"q": influxql, "db": self.dbname},
            headers=self.headers
        )
        metrics = list()
        if response.status_code == 200 and len(response.json()["results"]) > 0:
            for series in response.json()["results"][0]["series"]:
                if ["group"] in series["values"]:
                    continue
                metrics.append(series["name"])
        return metrics

    @cached_property
    def _dbname(self):
        influxql = "SHOW DATABASES"
        response = self._get(
            f"{self.url}/query",
            params={"q": influxql},
        )
        if response.status_code == 200 and len(response.json()["results"]) > 0:
            return response.json()["results"][0]["series"][0]["values"][0][0]

