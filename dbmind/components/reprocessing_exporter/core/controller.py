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
from dbmind.common.http import HttpService, request_mapping, standardized_api_output
from dbmind.common.http import Response
from dbmind.common.http.dbmind_protocol import HighAvailabilityConfig
from . import service

app = HttpService('Reprocessing Exporter')
latest_version = 'v1'
api_prefix = '/%s/api' % latest_version


@app.route('/', methods=['GET', 'POST'])
def index(*args):
    return Response(
        'reprocessing exporter (DBMind) \n'
        'metric URI: /metrics \n'
        'check status URI: /check-status \n'
        'repair URI: /repair \n'
    )


@request_mapping(api_prefix + '/check-status', methods=['POST'], api=True)
@standardized_api_output
def check_status(high_availability_config: HighAvailabilityConfig = None):
    if high_availability_config:
        cmd = high_availability_config.cmd
    else:
        cmd = ''
    return service.check_status_reprocessing_exporter(cmd)


@request_mapping(api_prefix + '/repair', methods=['POST'], api=True)
@standardized_api_output
def repair_interface(high_availability_config: HighAvailabilityConfig = None):
    if high_availability_config:
        cmd = high_availability_config.cmd
    else:
        cmd = ''
    return service.repair_interface_reprocessing_exporter(cmd)


@app.route('/metrics', methods=['GET'])
def metrics(*args):
    return Response(service.query_all_metrics())


def run(host, port, ssl_keyfile, ssl_certfile, ssl_keyfile_password, ssl_ca_file):
    app.start_listen(host=host, port=port,
                     ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile,
                     ssl_keyfile_password=ssl_keyfile_password, ssl_ca_file=ssl_ca_file)
