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

import os
import shlex
import unittest

from dbmind.common.platform import LINUX
from dbmind.components import extract_log

os.umask(0o0077)
LOG_CONTENT = """2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140235329963776 0[0:0#0]  0 [INSTR] LOG:  [Statement] no free slot for statement entry!
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140233600366336 0[0:0#0]  2533275203877978 [BACKEND] LOG:  execute S_2: SELECT no_o_id     FROM bmsql_new_order     WHERE no_w_id = $1 AND no_d_id = $2     ORDER BY no_o_id ASC
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140233600366336 0[0:0#0]  2533275203877978 [BACKEND] DETAIL:  parameters: $1 = '6', $2 = '1'
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140237393622784 0[0:0#0]  0 [BACKEND] LOG:  execute S_1: START TRANSACTION
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140233650710272 0[0:0#0]  0 [INSTR] LOG:  [Statement] no free slot for statement entry!
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140234343827200 0[0:0#0]  2533275203877981 [BACKEND] LOG:  execute S_3: SELECT no_o_id     FROM bmsql_new_order     WHERE no_w_id = $1 AND no_d_id = $2     ORDER BY no_o_id ASC
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140234343827200 0[0:0#0]  2533275203877981 [BACKEND] DETAIL:  parameters: $1 = '9', $2 = '6'
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140238224094976 0[0:0#0]  0 [BACKEND] LOG:  execute S_2: ROLLBACK
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140238660302592 0[0:0#0]  2533275203877979 [BACKEND] LOG:  execute S_7: SELECT ol_i_id, ol_supply_w_id, ol_quantity,        ol_amount, ol_delivery_d     FROM bmsql_order_line     WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3     ORDER BY ol_w_id, ol_d_id, ol_o_id, ol_number
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140238660302592 0[0:0#0]  2533275203877979 [BACKEND] DETAIL:  parameters: $1 = '8', $2 = '9', $3 = '3009'
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140233512277760 0[0:0#0]  2533275203877980 [BACKEND] LOG:  execute S_3: SELECT no_o_id     FROM bmsql_new_order     WHERE no_w_id = $1 AND no_d_id = $2     ORDER BY no_o_id ASC
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140233512277760 0[0:0#0]  2533275203877980 [BACKEND] DETAIL:  parameters: $1 = '4', $2 = '9'
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140235263887104 0[0:0#0]  2533275203877963 [BACKEND] LOG:  execute S_3: SELECT no_o_id     FROM bmsql_new_order     WHERE no_w_id = $1 AND no_d_id = $2     ORDER BY no_o_id ASC
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140235263887104 0[0:0#0]  2533275203877963 [BACKEND] DETAIL:  parameters: $1 = '6', $2 = '2'
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140237091632896 0[0:0#0]  2533275203877982 [BACKEND] LOG:  execute S_7: SELECT o_id, o_entry_d, o_carrier_id     FROM bmsql_oorder     WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3       AND o_id = (          SELECT max(o_id)               FROM bmsql_oorder               WHERE o_w_id = $4 AND o_d_id = $5 AND o_c_id = $6          )
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140237091632896 0[0:0#0]  2533275203877982 [BACKEND] DETAIL:  parameters: $1 = '3', $2 = '1', $3 = '335', $4 = '3', $5 = '1', $6 = '335'
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140237584463616 0[0:0#0]  2533275203877983 [BACKEND] LOG:  execute S_8: SELECT ol_i_id, ol_supply_w_id, ol_quantity,        ol_amount, ol_delivery_d     FROM bmsql_order_line     WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3     ORDER BY ol_w_id, ol_d_id, ol_o_id, ol_number
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140237584463616 0[0:0#0]  2533275203877983 [BACKEND] DETAIL:  parameters: $1 = '4', $2 = '2', $3 = '307'
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140239271081728 0[0:0#0]  0 [BACKEND] LOG:  execute S_1: START TRANSACTION
2021-12-05 16:53:35.445 benchmarksql_ori_10 postgres 127.0.0.1 140233295787776 0[0:0#0]  0 [INSTR] LOG:  [Statement] no free slot for statement entry!
2021-12-05 16:53:35.446 benchmarksql_ori_10 postgres 127.0.0.1 140237775304448 0[0:0#0]  2533275203877985 [BACKEND] LOG:  execute S_3: SELECT no_o_id     FROM bmsql_new_order     WHERE no_w_id = $1 AND no_d_id = $2     ORDER BY no_o_id ASC
2021-12-05 16:53:35.446 benchmarksql_ori_10 postgres 127.0.0.1 140237775304448 0[0:0#0]  2533275203877985 [BACKEND] DETAIL:  parameters: $1 = '9', $2 = '5'
2021-12-05 16:53:35.446 benchmarksql_ori_10 postgres 127.0.0.1 140241489360640 0[0:0#0]  2533275203877986 [BACKEND] LOG:  execute S_4: SELECT no_o_id     FROM bmsql_new_order     WHERE no_w_id = $1 AND no_d_id = $2     ORDER BY no_o_id ASC
2021-12-05 16:53:35.446 benchmarksql_ori_10 postgres 127.0.0.1 140241489360640 0[0:0#0]  2533275203877986 [BACKEND] DETAIL:  parameters: $1 = '2', $2 = '10'
2021-12-05 16:53:35.446 benchmarksql_ori_10 postgres 127.0.0.1 140237611726592 0[0:0#0]  0 [BACKEND] LOG:  execute S_1: START TRANSACTION
2021-12-05 16:53:35.446 benchmarksql_ori_10 postgres 127.0.0.1 140235485185792 0[0:0#0]  2533275203877987 [BACKEND] LOG:  execute S_2: SELECT no_o_id     FROM bmsql_new_order     WHERE no_w_id = $1 AND no_d_id = $2     ORDER BY no_o_id ASC
2021-12-05 16:53:35.446 benchmarksql_ori_10 postgres 127.0.0.1 140235485185792 0[0:0#0]  2533275203877987 [BACKEND] DETAIL:  parameters: $1 = '10', $2 = '1'
2021-12-05 16:53:35.446 benchmarksql_ori_10 postgres 127.0.0.1 140234311317248 0[0:0#0]  0 [BACKEND] LOG:  execute S_2: ROLLBACK
2021-12-05 16:53:35.446 benchmarksql_ori_10 postgres 127.0.0.1 140235535529728 0[0:0#0]  2533275203877984 [BACKEND] LOG:  execute S_2: SELECT no_o_id     FROM bmsql_new_order     WHERE no_w_id = $1 AND no_d_id = $2     ORDER BY no_o_id ASC
2021-12-05 16:53:35.446 benchmarksql_ori_10 postgres 127.0.0.1 140235535529728 0[0:0#0]  2533275203877984 [BACKEND] DETAIL:  parameters: $1 = '3', $2 = '7'
2023-08-15 17:44:26.209 guow115 postgres [local] 139775991609088 0[0:0#0]  562949953426607 [BACKEND] STATEMENT:  select * from temptable where age=$1;
AI Watchdog [watchdog_warn]: Watchdog found an abnormal event TPS dips and didn't try to handle it. Probable cause: TPS value was abnormal. The last 20 TPS values are {3, 3, 3, 3, 3, 3, 3, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 3, 3}. No abnormal features found. Here are the details while event happend: tps = 3 tps_threshold = 2 threadpool_usage = 0 D_state_rate = 0 in_D_state = 0 cpu_usage = 0 feature_code = 0"""
LOGDIR = os.path.realpath('temp')
LOG = os.path.join(LOGDIR, 'test.log')
OUTPUT = os.path.join(LOGDIR, 'output')


class ExtractlogTester(unittest.TestCase):

    def tearDown(self):
        if not LINUX:
            return

        os.remove(LOG)
        os.remove(OUTPUT)
        os.rmdir(LOGDIR)

    def test_extract_log(self):
        if not LINUX:
            return

        if not os.path.exists(LOGDIR):
            os.mkdir(LOGDIR)
        with open(LOG, 'w') as file_h:
            file_h.write(LOG_CONTENT)

        cmd = f'{LOGDIR} {OUTPUT} \'%m %u %d %h %p %S \' --statement --max-template-num 3 --max-reserved-period 100 --start-time \'2021-12-04 16:53:35\' '
        extract_log.main(shlex.split(cmd))
