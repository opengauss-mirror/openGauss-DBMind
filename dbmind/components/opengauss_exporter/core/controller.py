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
from dbmind.common.http import HttpService
from dbmind.common.http import Response, JSONResponse
from . import service

app = HttpService('DBMind-openGauss-exporter')


@app.route('/', methods=['GET', 'POST'])
def index(*args):
    return Response(
        'openGauss exporter (DBMind)\n'
        'metric URI: /metrics \n'
        'info URI: /info'
    )


@app.route('/info', methods=['GET', 'POST'])
def info(*args):
    return JSONResponse(service.EXPORTER_FIXED_INFO)


def metrics(*args):
    return Response(service.query_all_metrics())


def bind_rpc_service(rpc_service, uri='/rpc'):
    def invoking_adaptor(_json: dict):
        return rpc_service.invoke_handler(_json)

    app.attach(invoking_adaptor, uri, methods=['POST'], api=True)


def run(host, port, telemetry_path, ssl_keyfile, ssl_certfile, ssl_keyfile_password, ssl_ca_file):
    app.attach(metrics, telemetry_path)
    app.start_listen(host, port, ssl_keyfile, ssl_certfile, ssl_keyfile_password, ssl_ca_file)
