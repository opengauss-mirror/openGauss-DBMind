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

from datetime import datetime, timedelta
from urllib.parse import urlparse

from dbmind.common.http.requests_utils import create_requests_session
from dbmind.common.utils import cached_property, cast_to_int_or_float

from .tsdb_client import TsdbClient, cast_duration_to_seconds
from ..exceptions import ApiClientException
from ..types import Sequence
from ..types.ssl import SSLContext


def _influxql_generator(
        metric_name: str,
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
    :param start_ts_in_s:  (float) A timestamp number in seconds that specifies the metric range start time.
    :param end_ts_in_s: (float) A timestamp number in seconds that specifies the metric range end time.
    :param label_config: (dict) A dictionary contains the exact filtering information.
    :param labels_like: (dict) A dictionary contains the partial filtering information in form of regex.
    :return: (list) A list of filtered sequences of the specified metric_name between the given time range.
    """

    def time_filter_generator(start_timestamp_in_ms, end_timestamp_in_ms):
        if start_timestamp_in_ms is None and end_timestamp_in_ms is None:
            # This branch is designed for getting the latest sequence. Since the collection interval is 15s,
            # 20s is the choice that ensures both query performance and accuracy, when LAST() function is applied.
            # It works well in the testing environment.
            return ["time >= NOW() - 20s"]
        time_filter = list()
        if start_timestamp_in_ms:
            time_filter.append(f"time >= {start_timestamp_in_ms}ms")
        if end_timestamp_in_ms:
            time_filter.append(f"time <= {end_timestamp_in_ms}ms")
        return time_filter

    def condition_filter_generator(tags, tags_like):
        filter_list = list()
        if tags:
            for k, v in tags.items():
                k = k.replace('\"', '\\"')
                v = v.replace("\'", "\\'")
                if k.endswith("!"):
                    filter_list.append(f""""{k}" != '{v}'""")
                else:
                    filter_list.append(f""""{k}" = '{v}'""")

        if tags_like:
            for k, v in tags_like.items():
                k = k.replace('\"', '\\"')
                v = v.replace("\'", "\\'")
                if k.endswith("!"):
                    filter_list.append(f""""{k}" !~ /{v}/""")
                else:
                    filter_list.append(f""""{k}" =~ /{v}/""")

        return filter_list

    filters = condition_filter_generator(label_config, labels_like)
    start_ts_in_ms = int(start_ts_in_s * 1000) if start_ts_in_s else None
    end_ts_in_ms = int(end_ts_in_s * 1000) if end_ts_in_s else None
    filters += time_filter_generator(start_ts_in_ms, end_ts_in_ms)
    filters = "WHERE " + " AND ".join(filters) if filters else ""
    group_by = "GROUP BY *"
    metric_name = metric_name.replace('\"', '\\"')
    influxql = f"""SELECT "value" FROM "{metric_name}" {filters} {group_by}"""

    return influxql


def remove_empty_key(tags):
    """ To remove the key with empty string value."""
    if not tags or not isinstance(tags, dict):
        return {}
    return {k: v for k, v in tags.items() if not (v is None or v == '')}


# Standardized the format of return value.
def _standardize(data, step=None):
    if step is not None:
        step = cast_to_int_or_float(step)
        step = step * 1000  # convert to ms
    rv = list()
    for d in data["results"]:
        if "error" in d:
            raise ValueError(d.get("error"))

        if "series" not in d:
            return list()

        for series in d["series"]:
            metric_name = series["name"]
            tags = series["tags"]
            series_values = list(zip(*series["values"]))
            if "__name__" in tags:
                tags.pop("__name__")
            tags = remove_empty_key(tags)

            rv.append(
                Sequence(
                    timestamps=tuple(int(item) for item in series_values[0]),
                    values=tuple(float(item) for item in series_values[1]),
                    name=metric_name,
                    labels=tags,
                    step=step,
                    align_timestamp=(step is not None)
                )
            )

    return rv


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
        """Functions as a Constructor for the class InfluxDB v1.8.10 Connect."""
        if url is None:
            raise TypeError("missing url")

        self.headers = headers
        self.url = url
        self._session_args = (username, password, ssl_context)
        self.influxdb_host = urlparse(self.url).netloc
        self._all_metrics = None

        self.dbname = dbname if dbname is not None else self._dbname

    def _get(self, url, **kwargs):
        with create_requests_session(*self._session_args) as session:
            return session.get(url=url, **kwargs)

    def check_connection(self, params: dict = None) -> bool:
        """
        Check InfluxDB connection.
        :param params: (dict) Optional dictionary containing parameters to be
            sent along with the API request.
        :returns: (bool) True if the endpoint can be reached, False if it cannot be reached.
        """
        response = self._get(
            f"{self.url}/ping",
            headers=self.headers,
            params=params
        )
        return response.ok

    def get_current_metric_value(
        self, metric_name: str, label_config: dict = None,
        min_value: float = None, max_value: float = None, params: dict = None
    ):
        r"""
        Get the current metric target for the specified metric and label configuration.
        :param metric_name: (str) The name of the metric
        :param label_config: (dict) A dictionary that specifies metric labels and their
            values
        :param min_value: filter sequence whose value is greater than min_value
        :param max_value: filter sequence whose value is less than max_value
        :param params: (dict) Optional dictionary containing GET parameters to be sent
            along with the API request, such as "epoch", "db"
        :returns: (list) A list of current metric values for the specified metric
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (ApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        labels_like = params.pop("labels_like") if "labels_like" in params else {}
        influxql = _influxql_generator(
            metric_name,
            start_ts_in_s=None,
            end_ts_in_s=None,
            label_config=label_config,
            labels_like=labels_like
        )
        influxql = influxql.replace("SELECT value", "SELECT LAST(value) AS value")
        # using the query API to get raw data
        response = self._get(
            f"{self.url}/query",
            params={**params, **{"q": influxql, "epoch": "ms", "db": self.dbname}},
            headers=self.headers
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
            start_time: datetime = (datetime.now() - timedelta(minutes=10)),
            end_time: datetime = datetime.now(),
            chunk_size: timedelta = None,
            step: str = None,
            min_value: float = None,
            max_value: float = None,
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
        :param min_value: filter sequence whose value is greater than min_value
        :param max_value: filter sequence whose value is less than max_value
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

        if not (isinstance(start_time, datetime) and isinstance(end_time, datetime)):
            raise TypeError("start_time and end_time can only be of type datetime.datetime")
        if start_time > end_time:
            return []

        start_ts_in_s = datetime.timestamp(start_time)
        end_ts_in_s = datetime.timestamp(end_time)
        influxql = _influxql_generator(
            metric_name,
            start_ts_in_s=start_ts_in_s,
            end_ts_in_s=end_ts_in_s,
            label_config=label_config,
            labels_like=labels_like
        )
        response = self._get(
            f"{self.url}/query",
            params={**params, **{"q": influxql, "epoch": "ms", "db": self.dbname}},
            headers=self.headers
        )
        if response.status_code == 200:
            data = response.json()
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )

        return _standardize(data, step=step or self.scrape_interval)

    def custom_query(self, query: str, timeout=None, params: dict = None):
        """
        Send an influxql to a InfluxDB Host.
        This method takes as input a string which will be sent as a query to
        the specified InfluxDB Host. This query is an influxql query.
        :param timeout: wait for query
        :param query: (str) This is an influxql query
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
                    return int((ts + 8 * 60 * 60) * 1000)
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
                if d.get("name") == "config-monitor":
                    interval = d.get("values")[0][d.get("columns").index("store-interval")]
                    return cast_duration_to_seconds(interval)
        return None

    @property
    def current_scrape_interval(self):
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

    @property
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

    @property
    def _dbname(self):
        influxql = "SHOW DATABASES"
        response = self._get(
            f"{self.url}/query",
            params={"q": influxql},
        )
        if response.status_code == 200 and len(response.json()["results"]) > 0:
            return response.json()["results"][0]["series"][0]["values"][-1][0]

    @property
    def name(self):
        return "influxdb"
