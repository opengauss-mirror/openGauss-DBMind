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
from datetime import datetime, timedelta
from urllib.parse import urlparse

from dbmind.common.http.requests_utils import create_requests_session
from dbmind.common.utils import cached_property

from .tsdb_client import TsdbClient, cast_duration_to_seconds
from ..exceptions import ApiClientException
from ..types import Sequence
from ..types.ssl import SSLContext


def label_to_query(labels: dict = None, labels_like: dict = None):
    query_list = list()
    if isinstance(labels, dict) and labels:
        for k, v in labels.items():
            query_list.append(f"{k}=\"{v}\"")
    if isinstance(labels_like, dict) and labels_like:
        for k, v in labels_like.items():
            query_list.append(f"{k}=~\"{v}\"")
    return "{" + ",".join(query_list) + "}"


# Standardized the format of return value.
def _standardize(data, step=None):
    if step is not None:
        step = step * 1000  # convert to ms
    rv = []
    for datum in data:
        if 'values' not in datum:
            datum['values'] = [datum.pop('value')]
        datum_metric = datum.get('metric') or {}
        datum_values = datum.get('values') or {}
        metric_name = datum_metric.pop('__name__', None)
        rv.append(
            Sequence(
                timestamps=tuple(int(item[0] * 1000) for item in datum_values),
                values=tuple(float(item[1]) for item in datum_values),
                name=metric_name,
                labels=datum_metric,
                step=step,
                align_timestamp=(step is not None)
            )
        )
    return rv

class PrometheusClient(TsdbClient):
    """
    A Class for collection of metrics from a Prometheus Host.
    :param url: (str) url for the prometheus host
    :param headers: (dict) A dictionary of http headers to be used to communicate with
        the host. Example: {"Authorization": "bearer my_oauth_token_to_the_host"}
    """

    def __init__(
            self,
            url: str,
            username: str = None,
            password: str = None,
            ssl_context: SSLContext = None,
            headers: dict = None,
    ):
        """Functions as a Constructor for the class PrometheusConnect."""
        if url is None:
            raise TypeError("missing url")

        self.headers = headers
        self.url = url
        self.prometheus_host = urlparse(self.url).netloc
        self._all_metrics = None

        self._session_args = (username, password, ssl_context)

    def _get(self, url, **kwargs):
        with create_requests_session(*self._session_args) as session:
            return session.get(url=url, **kwargs)

    def _post(self, url, **kwargs):
        with create_requests_session(*self._session_args) as session:
            return session.post(url=url, **kwargs)

    def check_connection(self, params: dict = None) -> bool:
        """
        Check Prometheus connection.
        :param params: (dict) Optional dictionary containing parameters to be
            sent along with the API request.
        :returns: (bool) True if the endpoint can be reached, False if cannot be reached.
        """
        response = self._get(
            "{0}/".format(self.url),
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
        :param min_value; filter the sequence whose value is greater than min_value
        :param max_value; filter the sequence whose value is less than max_value
        :param params: (dict) Optional dictionary containing GET parameters to be sent
            along with the API request, such as "time"
        :returns: (list) A list of current metric values for the specified metric
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (ApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        labels_like = params.pop('labels_like') if 'labels_like' in params else {}
        if label_config or labels_like:
            query = metric_name + label_to_query(label_config, labels_like)
        else:
            query = metric_name
        if min_value:
            query = str(min_value) + '<' + query
        if max_value:
            query = query + '<' + str(max_value)

        # using the query API to get raw data
        data = []
        response = self._get(
            "{0}/api/v1/query".format(self.url),
            params={**params, **{"query": query}},
            headers=self.headers,
        )
        if response.status_code == 200:
            data += response.json()["data"]["result"]
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
            request made to the prometheus host
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param min_value; filter the sequence whose value is greater than min_value
        :param max_value; filter the sequence whose value is less than max_value
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :return: (list) A list of metric data for the specified metric in the given time
            range
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (ApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        labels_like = params.pop('labels_like') if 'labels_like' in params else {}
        if label_config or labels_like:
            query = metric_name + label_to_query(label_config, labels_like)
        else:
            query = metric_name
        data = []
        if not (isinstance(start_time, datetime) and isinstance(end_time, datetime)):
            raise TypeError("start_time and end_time can only be of type datetime.datetime")

        start = round(start_time.timestamp())
        end = round(end_time.timestamp())
        if start > end:
            return data

        chunk_seconds = round((end_time - start_time).total_seconds())

        if step is None:
            # using the query API to get raw data
            response = self._get(
                "{0}/api/v1/query".format(self.url),
                params={
                    **params,
                    **{
                        "query": query + "[" + str(chunk_seconds) + "s" + "]",
                        "time": end,
                    }
                },
                headers=self.headers,
            )
        else:
            if min_value:
                query = str(min_value) + '<=' + query
            if max_value:
                query = query + '<' + str(max_value)
            # using the query_range API to get raw data
            response = self._get(
                "{0}/api/v1/query_range".format(self.url),
                params={**params, **{"query": query, "start": start, "end": end, "step": step}},
                headers=self.headers,
            )

        if response.status_code == 200:
            data += response.json()["data"]["result"]
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )

        logging.debug('Fetched sequence (%s) from tsdb from %s to %s. The length of sequence is %s.',
                      metric_name, start_time, end_time, len(data))
        return _standardize(data, step=step or self.scrape_interval)

    def delete_metric_data(self,
                           metric_name: str,
                           from_timestamp: int = None,
                           to_timestamp: int = None,
                           labels: dict = None,
                           labels_like: dict = None,
                           flush: bool = False
                           ):
        if from_timestamp > to_timestamp:
            raise ValueError("There is a problem with the start time being greater than the end time.")
        params = {}
        if from_timestamp is not None:
            params['start'] = from_timestamp
        if to_timestamp is not None:
            params['end'] = to_timestamp
        metric_filter_labels = label_to_query(labels, labels_like)
        filter_condition = metric_filter_labels if metric_filter_labels != '{}' else ''
        if metric_name is not None:
            params['match[]'] = "{0}{1}".format(metric_name, filter_condition)
        # using the query_range API to get raw data
        response = self._post(
            "{0}/api/v1/admin/tsdb/delete_series?{1}".format(self.url, urlencode(params)),
            headers=self.headers,
        )
        if response.status_code != 204:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )
        logging.debug('Delete sequences (%s) from tsdb from %s to %s.',
                      metric_name, from_timestamp, to_timestamp)
        if flush:
            response = self._post(
                "{0}/api/v1/admin/tsdb/clean_tombstones".format(self.url),
                headers=self.headers,
            )
            if response.status_code != 204:
                raise ApiClientException(
                    "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
                )
            logging.debug('TSDB data disk refresh')

    def custom_query(self, query: str, timeout=None, params: dict = None):
        """
        Send a custom query to a Prometheus Host.
        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query.
        :param query: (str) This is a PromQL query, a few examples can be found
            at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "time"
        :param timeout: how long to wait for query
        :returns: (list) A list of metric data received in response of the query sent
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        params = params or {}
        query = str(query)
        # using the query API to get raw data
        response = self._get(
            "{0}/api/v1/query".format(self.url),
            params={**params, **{"query": query}},
            headers=self.headers,
            timeout=timeout
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )

        return _standardize(data, step=params.get('step'))

    def custom_query_range(self, query: str, start_time: datetime, end_time: datetime,
                           step: str, params: dict = None):
        """
        Send a query_range to a Prometheus Host.
        This method takes as input a string which will be sent as a query to
        the specified Prometheus Host. This query is a PromQL query.
        :param query: (str) This is a PromQL query, a few examples can be found
            at https://prometheus.io/docs/prometheus/latest/querying/examples/
        :param start_time: (datetime) A datetime object that specifies the query range start time.
        :param end_time: (datetime) A datetime object that specifies the query range end time.
        :param step: (str) Query resolution step width in duration format or float number of seconds
        :param params: (dict) Optional dictionary containing GET parameters to be
            sent along with the API request, such as "timeout"
        :returns: (dict) A dict of metric data received in response of the query sent
        :raises:
            (RequestException) Raises an exception in case of a connection error
            (PrometheusApiClientException) Raises in case of non 200 response status code
        """
        start = round(start_time.timestamp())
        end = round(end_time.timestamp())
        params = params or {}
        query = str(query)
        # using the query_range API to get raw data
        response = self._get(
            "{0}/api/v1/query_range".format(self.url),
            params={**params, **{"query": query, "start": start, "end": end, "step": step}},
            headers=self.headers,
        )
        if response.status_code == 200:
            data = response.json()["data"]["result"]
        else:
            raise ApiClientException(
                "HTTP Status Code {} ({!r})".format(response.status_code, response.content)
            )
        return _standardize(data, step=step or self.scrape_interval)

    def timestamp(self):
        seq = self.get_current_metric_value('prometheus_remote_storage_highest_timestamp_in_seconds')
        if len(seq) == 0 or len(seq[0]) == 0:
            return 0
        return seq[0].timestamps[0]

    @cached_property
    def scrape_interval(self):
        response = self._get(
            "{0}/api/v1/label/interval/values".format(self.url),
            headers=self.headers
        ).json()
        if response['status'] == 'success' and len(response['data']) > 0:
            return cast_duration_to_seconds(response['data'][0])
        return None

    @cached_property
    def all_metrics(self):
        response = self._get(
            "{0}/api/v1/label/__name__/values".format(self.url),
            headers=self.headers
        ).json()
        if response['status'] == 'success' and len(response['data']) > 0:
            return response['data']
        return list()

    @cached_property
    def name(self):
        return 'prometheus'
