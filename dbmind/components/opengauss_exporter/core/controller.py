# Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
from dbmind.common.http import Response, JSONResponse
from dbmind.common.http.dbmind_protocol import HighAvailabilityConfig
from . import service

app = HttpService('DBMind-openGauss-exporter')
latest_version = 'v1'
api_prefix = '/%s/api' % latest_version


@app.route('/', methods=['GET', 'POST'])
def index(*args):
    return Response(
        'OpenGauss exporter (DBMind)\n'
        'metric URI: /metrics \n'
        'info URI: /info \n'
        'check status URI: /check-status \n'
        'repair URI: /repair \n'
    )


@app.route('/info', methods=['GET', 'POST'])
def info(*args):
    return JSONResponse(service.EXPORTER_FIXED_INFO)


@request_mapping(api_prefix + '/check-status', methods=['POST'], api=True)
@standardized_api_output
def check_status(high_availability_config: HighAvailabilityConfig = None):
    if high_availability_config:
        cmd = high_availability_config.cmd
    else:
        cmd = ''
    return service.check_status_opengauss_exporter(cmd)


@request_mapping(api_prefix + '/repair', methods=['POST'], api=True)
@standardized_api_output
def repair_interface(high_availability_config: HighAvailabilityConfig = None):
    if high_availability_config:
        cmd = high_availability_config.cmd
    else:
        cmd = ''
    return service.repair_interface_opengauss_exporter(cmd)


def metrics(*args):
    return Response(service.query_all_metrics())


def bind_rpc_service(rpc_service, uri='/rpc'):
    def invoking_adaptor(_json: dict):
        return rpc_service.invoke_handler(_json)

    app.attach(invoking_adaptor, uri, methods=['POST'], api=True)


def run(host, port, telemetry_path, ssl_keyfile, ssl_certfile, ssl_keyfile_password, ssl_ca_file):
    app.attach(metrics, telemetry_path)
    app.start_listen(host, port, ssl_keyfile, ssl_certfile, ssl_keyfile_password, ssl_ca_file)
