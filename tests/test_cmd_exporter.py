# Copyright (c) 2023 Huawei Technologies Co.,Ltd.
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

import argparse
import copy
import json
import os
import queue
from datetime import datetime
from unittest import mock

import yaml

from dbmind.common.platform import LINUX
from dbmind.components.cmd_exporter import controller, service
from dbmind.components.cmd_exporter.cli import ExporterMain
from dbmind.components.cmd_exporter.cmd_module import impl, utils
from dbmind.components.cmd_exporter.log_module import log_extractor
from dbmind.components.cmd_exporter.log_module.log_agent.inotify import IN_MODIFY
from dbmind.components.cmd_exporter.log_module.log_miner.consts import constants

SELF_PATH = os.path.dirname(__file__)
DBMIND_PATH = os.path.join(os.path.realpath(os.path.dirname(SELF_PATH)), 'dbmind')
YAML_PATH = os.path.join(DBMIND_PATH, 'components', 'cmd_exporter', "yamls", 'default.yml')
TEST_PATH = os.path.join(SELF_PATH, "test_cmd_exporter")
if not os.path.exists(TEST_PATH):
    os.mkdir(TEST_PATH)

LOG_PATH = os.path.join(TEST_PATH, "dbmind_cmd_exporter.log")
FFIC_PATH = os.path.join(TEST_PATH, 'ffic_log')
if not os.path.exists(FFIC_PATH):
    os.mkdir(FFIC_PATH)

with open(os.path.join(FFIC_PATH, 'ffic_opengauss.log'), 'w', errors='ignore') as f:
    f.writelines(
        "====== Statememt info ======\n"
        "[statement] unique SQL key - sql id: 0, cn id: 0, user id: 0\n"
        "[statement] debug query id: 0\n"
    )


CM_CTL_QUERY_CVIDP = """[  CMServer State   ]

node             node_ip         instance state
-------------------------------------------------
1  127.0.0.1 127.0.0.1   1  /cms  Standby
2  127.0.0.1 127.0.0.1   2  /cms  Primary
3  127.0.0.1 127.0.0.1   3  /cms  Standby

[    ETCD State     ]

node             node_ip         instance state
-------------------------------------------------------
1  127.0.0.1 127.0.0.1   7001  /etcd  StateLeader
2  127.0.0.1 127.0.0.1   7002  /etcd  StateFollower
3  127.0.0.1 127.0.0.1   7003  /etcd  StateFollower

[   Cluster State   ]

cluster_state   : Degraded
redistributing  : No
balanced        : No
current_az      : AZ_ALL

[ Coordinator State ]

node             node_ip         instance        state
-------------------------------------------------
1  127.0.0.1 127.0.0.1    5001 19080  /cn  Deleted
3  127.0.0.1 127.0.0.1    5002 19080  /cn  Normal

[ Central Coordinator State ]

node             node_ip         instance state
-------------------------------------------------
3  127.0.0.1 127.0.0.1   5002  /central  Normal

[     GTM State     ]

node             node_ip         instance state                    sync_state
---------------------------------------------------------------------------------
1  127.0.0.1 127.0.0.2   1001  /gtm  P Down    Disk damaged   Sync
2  127.0.0.1 127.0.0.2   1002  /gtm  S Primary Connection ok  Sync
3  127.0.0.1 127.0.0.2   1003  /gtm  S Standby Connection ok  Sync

[  Datanode State   ]

node             node_ip         instance        state            | node             node_ip         instance        state            | node             node_ip         instance        state
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1  127.0.0.1 127.0.0.1    6001 9080  /dn  P Down    Disk damaged | 2  127.0.0.1 127.0.0.1   6002 9080  /dn  S Primary Need repair(Disconnected) | 3  127.0.0.1 127.0.0.1   6003 9080  /dn  S Standby Normal
"""

ERR_CM_CTL_QUERY_CVIDP = """logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:19:32:03.244 tid=2433  DEBUG1: ip: "127.0.0.1", cmd: "cm_ctl query -Cvip".

logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:19:32:03.244 tid=2433  DEBUG1: etcdStr is 127.0.0.1:2379:etcd_7001:1:7001:lf; 127.0.0.1:2379:etcd_7002:2:7002:lf; 127.0.0.1:2379:etcd_7003:3:7003:lf; .

[  CMServer State   ]

node             node_ip         instance state
-------------------------------------------------
logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:19:32:03.246 tid=2433  DEBUG1: connect to cmserver success, remotehost is 127.0.0.1:5000.

logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:19:32:03.448 tid=2433  DEBUG1: connect to cmserver success, remotehost is 127.0.0.1:5000.

logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:19:32:03.651 tid=2433  DEBUG1: connect to cmserver success, remotehost is 127.0.0.1:5000.

1  127.0.0.1 127.0.0.1   1  /cms  Standby
2  127.0.0.1 127.0.0.1   2  /cms  Primary
3  127.0.0.1 127.0.0.1   3  /cms  Standby

[    ETCD State     ]

node             node_ip         instance state
-------------------------------------------------------
1  127.0.0.1 127.0.0.1   7001  /etcd  StateLeader
2  127.0.0.1 127.0.0.1   7002  /etcd  StateFollower
3  127.0.0.1 127.0.0.1   7003  /etcd  StateFollower

logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:00:00:00.000 tid=2433  DEBUG1: query_kerberos: KRB5_CONFIG get fail.

logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:00:00:00.000 tid=2433  DEBUG1: 464 : connect to cmserver failed: local cmserver is not the primary.

logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:00:00:00.000 tid=2433  DEBUG1: connect to cmserver success, remotehost is 127.0.0.1:5000.

logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:00:00:00.000 tid=2433  DEBUG1: [InitResStat] no custom resource config.

[   Cluster State   ]

cluster_state   : Degraded
redistributing  : No
balanced        : No
current_az      : AZ_ALL

[ Coordinator State ]

node             node_ip         instance        state
-------------------------------------------------
1  127.0.0.1 127.0.0.1    5001 19080  /cn  logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:00:00:00.000 tid=2433  DEBUG1: [GetCnStatus] undocumentedVersion=0, status=7, dbState=1, buildReason=0

Deleted
logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:00:00:00.000 tid=2433  DEBUG1: Coordinator State: node=1 nodeName=127.0.0.1 ip=127.0.0.1 port=19080 instanceId=5001 DataPath=/usr/local/cn status=Deleted

3  127.0.0.1 127.0.0.1   5002 19080  /cn  logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:00:00:00.000 tid=2433  DEBUG1: [GetCnStatus] undocumentedVersion=0, status=4, dbState=1, buildReason=0

Normal

[ Central Coordinator State ]

node             node_ip         instance state
-------------------------------------------------
3  127.0.0.1 127.0.0.1   5002  /central  Normal

[     GTM State     ]

node             node_ip         instance state                    sync_state
---------------------------------------------------------------------------------
1  127.0.0.1 127.0.0.2    1001  /gtm  P Down    logfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:00:00:00.000 tid=2433  DEBUG1: GTM State: node=1 nodeName=127.0.0.1 ip=127.0.0.1 instanceId=1001 DataPath=/gtm static_role=P role=Down connect_status=Disk damaged sync_mode=Sync

Disk damaged   Sync
2  127.0.0.1 127.0.0.2   1002  /gtm  S Primary Connection ok  Sync
3  127.0.0.1 127.0.0.2   1003  /gtm  S Standby Connection ok  Sync

[  Datanode State   ]

node             node_ip         instance        state            | node             node_ip         instance        state            | node             node_ip         instance        state
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1  127.0.0.1 127.0.0.1    6001 9080  /dn  P Down    Disk damagedlogfile_open could not open log file:current.log Permission denied.
write_log_file,log file is null now:00:00:00.000 tid=2433  DEBUG1: Datanode State: node=1 nodeName=127.0.0.1 ip=127.0.0.1 port=9080 instanceId=6001 DataPath=/dn_6001 static_role=P role=Down state=Disk damaged buildReason=Unknown

 | 2  127.0.0.1 127.0.0.1   6002 9080  /dn  S Primary Need repair(Disconnected) | 3  127.0.0.1 127.0.0.1   6003 9080  /dn  S Standby Normal
"""

PS_UX = """cent     12768 99.5  3.4 62404556 9053256 ?    Sl   Aug14 12874:29 /opengauss -D /dn -M pending
cent     12786  0.0  0.0 1435112 54188 ?       Sl   Aug14   0:00 opengauss fenced UDF master process
cent     33858  0.0  0.0  57920  1156 ?        Ss   Aug09   0:02 ssh-agent -a /home
"""

LSBLK = """NAME                 KNAME TYPE MOUNTPOINT
vda                  vda   disk
|-vda1               vda1  part /boot
|-vda2               vda2  part
| |-VolGroup-lv_root dm-0  lvm  /
| `-VolGroup-lv_tmp  dm-1  lvm  /tmp
`-vda3               vda3  part
  `-VolGroup-lv_root dm-0  lvm  /
vdb                  vdb   disk
|-vdb1               vdb1  part /var/chroot/var/lib/log
|-vdb2               vdb2  part /var/chroot/usr/local
`-vdb3               vdb3  part /var/chroot/var/lib/engine/data1
"""

DF = """Filesystem                    1K-blocks      Used Available Use% Mounted on
/dev/mapper/VolGroup-lv_root   38114720  11440560  25012616  32% /
devtmpfs                       16369616         0  16369616   0% /dev
tmpfs                          16381756        72  16381684   1% /dev/shm
tmpfs                          16381756      1256  16380500   1% /run
tmpfs                          16381756         0  16381756   0% /sys/fs/cgroup
/dev/vda1                        999320     51492    879016   6% /boot
/dev/mapper/VolGroup-lv_tmp     1998672      6300   1871132   1% /tmp
/dev/vdb2                      65531436   6059428  56100184  10% /var/chroot/usr/local
/dev/vdb1                      32765712    534224  30537384   2% /var/chroot/var/lib/log
/dev/vdb3                     102687672   2070128  95358280   3% /var/chroot/var/lib/engine/data1
tmpfs                           3276352         0   3276352   0% /run/user/1000
127.0.0.1:/usr1/mnt/data  1031073536 796552944 182091840  82% /mnt/data
tmpfs                           3276352         0   3276352   0% /run/user/0
"""

TEST_LOG_LINES = {
    "/test/postgresql.log": [
        '{date} {time} dn_6001_6002_6003 duecparl hsccduecdb 192.168.0.1 140162661480192 3339435[0:0#0] '
        '65062961 PostgreSQL JDBC Driver 1125900874146756 [BACKEND] ERROR:  deadlock detected',
        '{date} {time} dn_6001_6002_6003 duecparl hsccduecdb 192.168.0.1 140162661480192 3339435[0:0#0] '
        '65062961 PostgreSQL JDBC Driver 1125900874146756 [BACKEND] DETAIL:  '
        'Process 140162661480192 waits for ShareLock on transaction 65062974; '
        'blocked by process 140149035517696. '
        'Process 140149035517696 waits for ShareLock on transaction 65062961; '
        'blocked by process 140162661480192. '
        'Process 140162661480192: UPDATE DUC_CONFIG_INSTANCE_ATT_T t0 '
        'SET LAST_UPDATE_DATE=$1, DELETE_FLAG=$2, INVOICING_STATUS=$3 '
        'WHERE t0.CONFIG_INSTANCE_ID = $4 and t0.route_id = $5 '
        'Process 140149035517696: UPDATE DUC_CONFIG_INSTANCE_ATT_T t0 '
        'SET LAST_UPDATE_DATE=$1, DELETE_FLAG=$2, INVOICING_STATUS=$3 '
        'WHERE t0.CONFIG_INSTANCE_ID = $4 and t0.route_id = $5',
        '{date} {time} dn_6001_6002_6003 dbmind2 postgres 192.168.0.1 140391467968256 1381864[0:0#0] '
        '0 dn_6002 0 [BACKEND] ERROR:  canceling statement due to user request',
        '{date} {time} dn_6001_6002_6003 dbmind2 postgres 192.168.0.1 140391467968256 1381864[0:0#0] '
        '0 dn_6002 0 [BACKEND] FATAL:  Invalid username/password,login denied.',
        '{date} {time} dn_6001_6002_6003 dbmind metadatabase100 192.168.0.1 140418251679488 14071867[0:0#0] '
        '0 DBMind-openGauss-exporter 0 [BACKEND] ERROR:  syntax error at or near ";" at character 37',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 [unknown] 0 [BACKEND] LOG: walreceiver could not connect and shutting down',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 [unknown] 0 [BACKEND] PANIC: shutting down',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 [unknown] 0 [BACKEND] ERROR:  Lock wait timeout: thread 139940914525952 on node cn_5002 '
        'waiting for AccessExclusiveLock on relation 24688 of database 15954 after 2000.064 ms',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 [unknown] 0 [BACKEND] DETAIL:  blocked by hold lock thread 139940845319936, '
        'statement <insert into houlei values(generate_series(1,2000000));>, '
        'hold lockmode RowExclusiveLock.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 [unknown] 0 [BACKEND] ERROR: Log control take effect due to RPO, target_rpo is 100, '
        'current_rpo is 100, current the sleep time is 400 microseconds',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 [unknown] 0 [BACKEND] ERROR: Log control take effect due to RTO, target_rto is 100, '
        'current_rto is 100, current the sleep time is 400 microseconds',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 [unknown] 0 [BACKEND] FATAL: [recovery_undo_zone:902]Undo meta CRC calculated(869753400) '
        'is different from CRC recorded(0) in page. zoneId 56, count 7',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 [unknown] 0 [BACKEND] ERROR: [OpenUndoFile:465]could not open undo file '
        '"undo/permanent/00004.meta.0000051": Permission denied',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  attempting to remove WAL segments older '
        'than log file 000000010000001C00000002',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: replication slot limit] max replication slot limits is 4, '
        'keep lsn is 4/8AE62280.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  keep all the xlog segments, because there is a full-build task in the backend, '
        'and start segno is less than or equal to zero',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: FullBuildXlogCopyStart] xlogcopystartptr is 6229, '
        'xlogcopystartptr: 4/8EE62280.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: DCF ] min applied segno is 6221, min applied lsn is 4/8BE62280.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: DCF] keep all files, segno is 6222.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: PRIMARY_MODE] DUMMY_STANDYS_MODE keep all log, segno is 6223.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: cbm] cbm tracked segno is 6224, cbm tracked lsn: 4/8CE62280.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: STANDBY && !dummyStandby] standby backup slot segno is 6225.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: aux db] keep all files, segno is 6226.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: EXTRO_READ] recycle segno is 0, keep all files, segno is 6227.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: EXTRO_READ] recycle segno is 6228, global recycle lsn: 4/8DE62280.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] LOG:  [recycle: PRIMARY_MODE] quorum/tools min required required, '
        'segno is 6230, quorum min lsn: 4/8EF62280, min tools required: 4/8FF62280.',
        '{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] '
        '0 dn_6001 0 [BACKEND] FATAL:  terminate because authentication timeout(60s)',
    ],
    "cm/cm_agent/cm_agent.log": [
        "{date} {time} tid=45356 SendCmsMsg LOG: [DiskUsageCheckAndReport]"
        "[line:116] Disk Usage status check thread start.",
        "{date} {time} tid=45356 SendCmsMsg LOG: cn restart !",
        '{date} {time} tid=45356 SendCmsMsg LOG: CN START system(command:/usr/local/core/app/bin/opengauss   '
        '--coordinator -D /usr/local/cn>> "/var/lib/log/Ruby/cm/cm_agent/system_call-current.log" 2>&1 &)."',
        "{date} {time} tid=45356 SendCmsMsg LOG: cn_manual_stop=0, cn_replace=0, g_cnDiskDamage=0, "
        "g_cnNicDown=0, port_conflict(9080)=0.",
        "{date} {time} tid=45356 SendCmsMsg LOG: cn_manual_stop=1, cn_replace=0, g_cnDiskDamage=0, "
        "g_cnNicDown=0, port_conflict(9080)=0.",
        "{date} {time} tid=45356 SendCmsMsg LOG: cn_manual_stop=0, cn_replace=0, g_cnDiskDamage=1, "
        "g_cnNicDown=0, port_conflict(9080)=0.",
        "{date} {time} tid=45356 SendCmsMsg LOG: cn_manual_stop=0, cn_replace=0, g_cnDiskDamage=0, "
        "g_cnNicDown=1, port_conflict(9080)=0.",
        "{date} {time} tid=45356 SendCmsMsg LOG: cn_manual_stop=0, cn_replace=0, g_cnDiskDamage=0, "
        "g_cnNicDown=0, port_conflict(9080)=1.",
        "{date} {time} tid=45356 SendCmsMsg LOG: datanodeId=0, dn_manual_stop=1, g_dnDiskDamage=0, "
        "g_nicDown=0, port_conflict=0, g_dnBuild=0, g_dnStartCounts=0",
        "{date} {time} tid=45356 SendCmsMsg LOG: datanodeId=0, dn_manual_stop=0, g_dnDiskDamage=1, "
        "g_nicDown=0, port_conflict=0, g_dnBuild=0, g_dnStartCounts=0",
        "{date} {time} tid=45356 SendCmsMsg LOG: datanodeId=0, dn_manual_stop=0, g_dnDiskDamage=0, "
        "g_nicDown=1, port_conflict=0, g_dnBuild=0, g_dnStartCounts=0",
        "{date} {time} tid=45356 SendCmsMsg LOG: datanodeId=0, dn_manual_stop=0, g_dnDiskDamage=0, "
        "g_nicDown=0, port_conflict=1, g_dnBuild=0, g_dnStartCounts=0",
        "{date} {time} tid=45356 SendCmsMsg LOG: gtm_manual_stop=1, gtm_replace=0, g_gtmDiskDamage=0, "
        "g_gtmNicDown=0, port_conflict(9080)=0.",
        "{date} {time} tid=45356 SendCmsMsg LOG: gtm_manual_stop=0, gtm_replace=0, g_gtmDiskDamage=1, "
        "g_gtmNicDown=0, port_conflict(9080)=0.",
        "{date} {time} tid=45356 SendCmsMsg LOG: gtm_manual_stop=0, gtm_replace=0, g_gtmDiskDamage=0, "
        "g_gtmNicDown=1, port_conflict(9080)=0.",
        "{date} {time} tid=45356 SendCmsMsg LOG: gtm_manual_stop=0, gtm_replace=0, g_gtmDiskDamage=0, "
        "g_gtmNicDown=0, port_conflict(9080)=1.",
        "{date} {time} tid=45356 SendCmsMsg LOG: GTM START system(command:ps -ux | grep 1)",
        "{date} {time} tid=45356 SendCmsMsg LOG: gtm restart !",
        "{date} {time} tid=45356 SendCmsMsg LOG: data path disc writable test failed, so what???.",
        "{date} {time} tid=45356 SendCmsMsg LOG: device cn1, [Io util: 100%%]",
        "{date} {time} tid=45356 SendCmsMsg LOG: device cn1, [Cpu util: 3%%], [Io util: 100%%]",
        "{date} {time} tid=45356 SendCmsMsg LOG: device dn2, [Cpu util: 12%%], [Io util: 58%%]",
        "{date} {time} tid=45356 AutoRepairCn ASYN LOG: [autorepaircn] cn_5002 can not be repaired: g_cnDiskDamage=1, "
        "g_cnNicDown=1, nodeFault=0, g_cnDiskFull=2",
    ],
    "cm/cm_server/cm_server.log": [
        '{date} {time} tid=30293 MAIN LOG: LD_LIBRARY_PATH=/usr1/cn/wisequery/script/gspylib/clib:/usr1/cn/cluster/',
        "{date} {time} tid=30293 MAIN LOG: restart 6001, there is not report msg for 111 sec.",
        "{date} {time} tid=30293 MAIN LOG: phony dead times(12:345 23) already exceeded, will restart(6001)",
        "{date} {time} tid=30293 MAIN LOG: cn_5001 is down. cn_down_to_delete=1, "
        "isCnDnDisconnected=0, cmd_disable_cn=0",
        "{date} {time} tid=30293 MAIN LOG: cn_5002 is down. cn_down_to_delete=0, "
        "isCnDnDisconnected=1, cmd_disable_cn=1",
        "{date} {time} tid=30293 MAIN LOG: the number 12 of cn instance restarts "
        "within ten minutes is more than 1212, and will be dropped.",
        "{date} {time} tid=30293 MAIN LOG: something happenned, restart to pending",
        "{date} {time} tid=30293 MAIN LOG: instance(6001) heartbeat timeout, heartbeat:21ffm, threshold:121234c",
        "{date} {time} tid=30293 MAIN LOG: [IsReadOnlySetByCM] instanceId: 6001 was set read only by cm",
        "{date} {time} tid=30293 MAIN LOG: [ReadOnlyActDoNoting] instance 6001 is transaction read only, "
        "disk_usage:85, read_only_threshold:85",
        "{date} {time} tid=30293 MAIN LOG: [ReadOnlyActSetReadOnlyOff] instance 6001 is read only, ddb is 1, "
        "need set ddb to 2, disk_usage:85, read_only_threshold:85,",
        "{date} {time} tid=30293 MAIN LOG: [ReadOnlyActSetDdbTo1Conditional] instance 6001 set read only manually, "
        "disk_usage:85, read_only_threshold:85",
        "{date} {time} tid=18248 AGENT_WORKER LOG: [Pending], line 1630: notify local datanode(6001) to standby.",
        "{date} {time} tid=30293 MAIN LOG: [IsReadOnlySetByCM] Set database to read only mode, "
        "instanceId=6001, usage=0.91",
        "{date} {time} tid=30293 MAIN LOG: [IsReadOnlySetByCM] Set database to read only mode, "
        "instanceId=6001, usage=0.92",
        "{date} {time} tid=30293 MAIN LOG: [IsReadOnlySetByCM] Set database to read only mode, "
        "instanceId=6001, usage=0.93",
        "{date} {time} tid=30293 MAIN LOG: [IsReadOnlySetByCM] Set database to read only mode, "
        "instanceId=6001, usage=0.94",
    ],
    "cm/etcd/etcd.log": [
        '{{"level":"info","ts":"{date}T{time}+0800","caller":"rafthttp/pipeline.go:72",'
        '"msg":"started HTTP pipelining with remote peer","local-member-id":"829aa03704834b23",'
        '"remote-peer-id":"27671f368b6129a2"}}',
        '{{"level":"info","ts":"{date}T{time}+0800","caller":"rafthttp/pipeline.go:72",'
        '"msg":"restarting local member","local-member-id":"829aa03704834b23",'
        '"remote-peer-id":"27671f368b6129a2"}}',
        '{{"level":"info","ts":"{date}T{time}+0800","caller":"rafthttp/pipeline.go:72",'
        '"msg":"prober detected unhealthy status","local-member-id":"829aa03704834b23",'
        '"remote-peer-id":"27671f368b6129a2"}}',
        '{{"level":"info","ts":"{date}T{time}+0800","caller":"rafthttp/pipeline.go:72",'
        '"msg":"authentication handshake failed","local-member-id":"829aa03704834b23",'
        '"remote-peer-id":"27671f368b6129a2"}}',
        '{{"level":"info","ts":"{date}T{time}+0800","caller":"rafthttp/pipeline.go:72",'
        '"msg":"leader is overloaded likely from slow disk","local-member-id":"829aa03704834b23",'
        '"remote-peer-id":"27671f368b6129a2"}}',
        '{{"level":"info","ts":"{date}T{time}+0800","caller":"rafthttp/pipeline.go:72",'
        '"msg":"the process took too long to overcome it, so we stop it like 112233",'
        '"local-member-id":"829aa03704834b23","remote-peer-id":"27671f368b6129a2"}}',
        '{{"level":"info","ts":"{date}T{time}+0800","caller":"rafthttp/pipeline.go:72",'
        '"msg":"slow fdatasync","local-member-id":"829aa03704834b23",'
        '"remote-peer-id":"27671f368b6129a2"}}',
        '{{"level":"info","ts":"{date}T{time}+0800","caller":"rafthttp/pipeline.go:72",'
        '"msg":"no space left on device","local-member-id":"829aa03704834b23",'
        '"remote-peer-id":"27671f368b6129a2"}}',
    ],
    "cm/om_monitor/om_monitor.log": [
        '{date} {time} tid=10129  LOG: LD_LIBRARY_PATH=/usr1/cn/wisequery/script/gspylib/clib',
        "{date} {time} tid=10129  LOG: kill etcd! command=ps -ux | grep 111",
    ],
    "cm/cm_agent/system_call.log": [
        "{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] 0 [unknown]"
        " 0 [BACKEND] LOG:  [Alarm Module]Host IP: linux130. Copy hostname directly in case",
        "{date} {time} cn_5001 [unknown] [unknown] localhost 140267004099008 0[0:0#0] 0 [unknown]"
        " 0 [BACKEND] LOG:  could not bind IPV4 socket: so we do sth",
    ],
    "pg_log/gtm/gtm.log": [
        '1:281471651729952:{date} {time} -LOG:  could not connect to the GTM primary server, '
        'the connection info: localhost=192.168.0.1 localport=6001 host=192.168.0.1 port=6001 '
        'node_name=gtm_1002 remote_type=6 connect_timeout=5: timeout expired',
        "1:281471651729952:{date} {time} -LOG: could not connect to the GTM primary server",
        "1:281471651729952:{date} {time} -FATAL: 000 011 111",
        "1:281471651729952:{date} {time} -PANIC: 000 123 111",
    ],
    os.path.join(FFIC_PATH, 'ffic_opengauss.log'): [
        "{date} {time} anything"
    ],
    "cm/cm_agent/system_alarm.log": [
        '{{"id":"0000001078919306","name":"AbnormalTopologyConnect","level":"notice",'
        '"scope":["newsql"],"source_tag":"kwephis21731672-cn_5001(1-2)","op_type":"firing",'
        '"details":"abnormal topology connection ,reason :cn_5001 can not connect to dn_6001",'
        '"clear_type":"ADAC","start_timestamp":{ts},"end_timestamp":0}}'
    ]
}

EXPECTED = [
    b'# HELP opengauss_cluster_state cluster state, 0 meaning abnormal and 1 meaning normal',
    b'# TYPE opengauss_cluster_state gauge',
    b'opengauss_cluster_state{abnormal="127.0.0.1:9080,127.0.0.1:9080",'
    b'central_cn_state="['
    b'{\\"ip\\": \\"127.0.0.1\\", \\"instance_id\\": \\"5002\\", \\"path\\": \\"/central\\", '
    b'\\"state\\": \\"Normal\\"}]",'
    b'cms_state="['
    b'{\\"ip\\": \\"127.0.0.1\\", \\"path\\": \\"/cms\\", \\"state\\": \\"Standby\\"}, '
    b'{\\"ip\\": \\"127.0.0.1\\", \\"path\\": \\"/cms\\", \\"state\\": \\"Primary\\"}, '
    b'{\\"ip\\": \\"127.0.0.1\\", \\"path\\": \\"/cms\\", \\"state\\": \\"Standby\\"}]",'
    b'cn_state="['
    b'{\\"ip\\": \\"127.0.0.1\\", \\"instance_id\\": \\"5001\\", \\"port\\": \\"19080\\", '
    b'\\"path\\": \\"/cn\\", \\"state\\": \\"Deleted\\"}, '
    b'{\\"ip\\": \\"127.0.0.1\\", \\"instance_id\\": \\"5002\\", \\"port\\": \\"19080\\", '
    b'\\"path\\": \\"/cn\\", \\"state\\": \\"Normal\\"}]",'
    b'dn_state="['
    b'{\\"ip\\": \\"127.0.0.1\\", \\"instance_id\\": \\"6001\\", \\"port\\": \\"9080\\", '
    b'\\"path\\": \\"/dn\\", \\"role\\": \\"Down\\", \\"state\\": \\"Disk damaged\\"}, '
    b'{\\"ip\\": \\"127.0.0.1\\", \\"instance_id\\": \\"6002\\", \\"port\\": \\"9080\\", '
    b'\\"path\\": \\"/dn\\", \\"role\\": \\"Primary\\", \\"state\\": \\"Need repair\\"}, '
    b'{\\"ip\\": \\"127.0.0.1\\", \\"instance_id\\": \\"6003\\", \\"port\\": \\"9080\\", '
    b'\\"path\\": \\"/dn\\", \\"role\\": \\"Standby\\", \\"state\\": \\"Normal\\"}]",'
    b'etcd_state="['
    b'{\\"ip\\": \\"127.0.0.1\\", \\"instance_id\\": \\"7001\\", \\"path\\": \\"/etcd\\", '
    b'\\"state\\": \\"StateLeader\\"}, '
    b'{\\"ip\\": \\"127.0.0.1\\", \\"instance_id\\": \\"7002\\", \\"path\\": \\"/etcd\\", '
    b'\\"state\\": \\"StateFollower\\"}, '
    b'{\\"ip\\": \\"127.0.0.1\\", \\"instance_id\\": \\"7003\\", \\"path\\": \\"/etcd\\", '
    b'\\"state\\": \\"StateFollower\\"}]",'
    b'from_instance="127.0.0.1",'
    b'gtm_state="['
    b'{\\"ip\\": \\"127.0.0.2\\", \\"instance_id\\": \\"1001\\", \\"path\\": \\"/gtm\\", '
    b'\\"role\\": \\"Down\\", \\"state\\": \\"Disk damaged\\", \\"sync_state\\": \\"Sync\\"}, '
    b'{\\"ip\\": \\"127.0.0.2\\", \\"instance_id\\": \\"1002\\", \\"path\\": \\"/gtm\\", '
    b'\\"role\\": \\"Primary\\", \\"state\\": \\"Connection ok\\", \\"sync_state\\": \\"Sync\\"}, '
    b'{\\"ip\\": \\"127.0.0.2\\", \\"instance_id\\": \\"1003\\", \\"path\\": \\"/gtm\\", '
    b'\\"role\\": \\"Standby\\", \\"state\\": \\"Connection ok\\", \\"sync_state\\": \\"Sync\\"}]",'
    b'normal="127.0.0.1:9080",primary="127.0.0.1:9080",standby="127.0.0.1:9080"} 0.0',
    b'# HELP opengauss_process_cpu_usage cpu usage of opengauss process',
    b'# TYPE opengauss_process_cpu_usage gauge',
    b'opengauss_process_cpu_usage{cwd="/home/1",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="11",port="19080",role="cn",user="cent"} 99.5',
    b'opengauss_process_cpu_usage{cwd="/home/2",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="22",port="9080",role="dn",user="cent"} 0.0',
    b'opengauss_process_cpu_usage{cwd="/home/3",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="33",port="9080",role="dn",user="cent"} 0.0',
    b'opengauss_process_cpu_usage{cwd="/home/4",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="44",port="9080",role="dn",user="cent"} 0.0',
    b'# HELP opengauss_process_mem_usage mem usage of opengauss process',
    b'# TYPE opengauss_process_mem_usage gauge',
    b'opengauss_process_mem_usage{cwd="/home/1",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="11",port="19080",role="cn",user="cent"} 3.4',
    b'opengauss_process_mem_usage{cwd="/home/2",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="22",port="9080",role="dn",user="cent"} 0.0',
    b'opengauss_process_mem_usage{cwd="/home/3",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="33",port="9080",role="dn",user="cent"} 0.0',
    b'opengauss_process_mem_usage{cwd="/home/4",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="44",port="9080",role="dn",user="cent"} 0.0',
    b'# HELP opengauss_process_leaked_fds leaked fds of opengauss process',
    b'# TYPE opengauss_process_leaked_fds gauge',
    b'opengauss_process_leaked_fds{cwd="/home/1",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="11",port="19080",role="cn",user="cent"} 0.0',
    b'opengauss_process_leaked_fds{cwd="/home/2",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="22",port="9080",role="dn",user="cent"} 0.0',
    b'opengauss_process_leaked_fds{cwd="/home/3",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="33",port="9080",role="dn",user="cent"} 0.0',
    b'opengauss_process_leaked_fds{cwd="/home/4",from_instance="127.0.0.1",ip="127.0.0.1",'
    b'pid="44",port="9080",role="dn",user="cent"} 0.0',
    b'# HELP opengauss_ping_state check if network is ok between all nodes',
    b'# TYPE opengauss_ping_state gauge',
    b'opengauss_ping_state{from_instance="127.0.0.1",source="127.0.0.1",target="192.168.0.2",'
    b'to_primary_dn="True"} 1.0',
    b'# HELP opengauss_ping_packet_rate get the packet rate between all nodes',
    b'# TYPE opengauss_ping_packet_rate gauge',
    b'opengauss_ping_packet_rate{from_instance="127.0.0.1",source="127.0.0.1",target="192.168.0.2",'
    b'to_primary_dn="True"} 1.0',
    b'# HELP opengauss_ping_lag get the lag between all nodes',
    b'# TYPE opengauss_ping_lag gauge',
    b'opengauss_ping_lag{from_instance="127.0.0.1",source="127.0.0.1",target="192.168.0.2",'
    b'to_primary_dn="True"} 1.0',
    b'# HELP opengauss_mount_usage verify the mount usage for every single node',
    b'# TYPE opengauss_mount_usage gauge',
    b'opengauss_mount_usage{device="vda",file_system="/dev/mapper/VolGroup-lv_root",from_instance="127.0.0.1",'
    b'instance_id="6001",ip="127.0.0.1",kernel_name="dm-0",path="/cn",port="5001",role="cn"} 32.0',
    b'opengauss_mount_usage{device="vda",file_system="/dev/mapper/VolGroup-lv_root",from_instance="127.0.0.1",'
    b'instance_id="6001",ip="127.0.0.1",kernel_name="dm-0",path="/cn",port="5002",role="cn"} 32.0',
    b'opengauss_mount_usage{device="vda",file_system="/dev/mapper/VolGroup-lv_root",from_instance="127.0.0.1",'
    b'instance_id="6001",ip="127.0.0.1",kernel_name="dm-0",path="/dn",port="6001",role="dn"} 32.0',
    b'opengauss_mount_usage{device="vda",file_system="/dev/mapper/VolGroup-lv_root",from_instance="127.0.0.1",'
    b'instance_id="6001",ip="127.0.0.1",kernel_name="dm-0",path="/dn",port="6002",role="dn"} 32.0',
    b'opengauss_mount_usage{device="vda",file_system="/dev/mapper/VolGroup-lv_root",from_instance="127.0.0.1",'
    b'instance_id="6001",ip="127.0.0.1",kernel_name="dm-0",path="/dn",port="6003",role="dn"} 32.0',
    b'# HELP opengauss_xlog_count get the count of the xlog files',
    b'# TYPE opengauss_xlog_count gauge',
    b'opengauss_xlog_count{device="vda",from_instance="127.0.0.1:9080",instance_id="6001",'
    b'path="/dn",role="dn"} 0.0',
    b'# HELP opengauss_xlog_oldest_lsn get the oldest lsn of the xlog files',
    b'# TYPE opengauss_xlog_oldest_lsn gauge',
    b'opengauss_xlog_oldest_lsn{device="vda",from_instance="127.0.0.1:9080",instance_id="6001",'
    b'path="/dn",role="dn"} 0.0',
    b'# HELP opengauss_nic_state check the count of the xlog files',
    b'# TYPE opengauss_nic_state gauge',
    b'opengauss_nic_state{from_instance="127.0.0.1",ip="[\\"127.0.0.1\\", \\"127.0.0.1\\"]"} 1.0',
    b'# HELP opengauss_log_deadlock_count the count of deadlocks during the scape interval.',
    b'# TYPE opengauss_log_deadlock_count gauge',
    b'opengauss_log_deadlock_count{app_name="PostgreSQL JDBC Driver",content='
    b'"Process 140162661480192 waits for ShareLock on transaction 65062974; blocked by process 140149035517696. '
    b'Process 140149035517696 waits for ShareLock on transaction 65062961; blocked by process 140162661480192. '
    b'Process 140162661480192: UPDATE DUC_CONFIG_INSTANCE_ATT_T t0 SET LAST_UPDATE_DATE=$1, DELETE_FLAG=$2, '
    b'INVOICING_STATUS=$3 WHERE t0.CONFIG_INSTANCE_ID = $4 and t0.route_id = $5 '
    b'Process 140149035517696: UPDATE DUC_CONFIG_INSTANCE_ATT_T t0 SET LAST_UPDATE_DATE=$1, '
    b'DELETE_FLAG=$2, INVOICING_STATUS=$3 WHERE t0.CONFIG_INSTANCE_ID = $4 and t0.route_id = $5",'
    b'datname="hsccduecdb",from_instance="127.0.0.1",host="192.168.0.1",node_name="dn_6001_6002_6003",'
    b'thread_name="BACKEND",user_name="duecparl"} 1.0',
    b'# HELP opengauss_log_user_cancel_statement the count of canceling statements during the scape interval.',
    b'# TYPE opengauss_log_user_cancel_statement gauge',
    b'opengauss_log_user_cancel_statement{app_name="dn_6002",datname="postgres",from_instance="127.0.0.1",'
    b'host="192.168.0.1",node_name="dn_6001_6002_6003",thread_name="BACKEND",user_name="dbmind2"} 1.0',
    b'# HELP opengauss_log_login_denied the count of logins denied during the scape interval.',
    b'# TYPE opengauss_log_login_denied gauge',
    b'opengauss_log_login_denied{app_name="dn_6002",datname="postgres",from_instance="127.0.0.1",host="192.168.0.1",'
    b'node_name="dn_6001_6002_6003",thread_name="BACKEND",user_name="dbmind2"} 1.0',
    b'# HELP opengauss_log_errors_total the count of errors in log file.',
    b'# TYPE opengauss_log_errors_total gauge',
    b'opengauss_log_errors_total{from_instance="127.0.0.1"} 11.0',
    b'# HELP opengauss_log_panic the count of panics in log file.',
    b'# TYPE opengauss_log_panic gauge',
    b'opengauss_log_panic{app_name="[unknown]",content="shutting down",datname="[unknown]",from_instance="127.0.0.1",'
    b'host="localhost",node_name="cn_5001",thread_name="BACKEND",user_name="[unknown]"} 1.0',
    b'# HELP opengauss_log_dn_ping_standby the count of dn ping standby.',
    b'# TYPE opengauss_log_dn_ping_standby gauge',
    b'opengauss_log_dn_ping_standby{from_instance="127.0.0.1"} 1.0',
    b'# HELP opengauss_log_recycle_lsn the count of xlog recycle with lsn.',
    b'# TYPE opengauss_log_recycle_lsn gauge',
    b'opengauss_log_recycle_lsn{from_instance="127.0.0.1",node_name="cn_5001",'
    b'recycle_lsn="000000010000001C00000002"} 7170.0',
    b'# HELP opengauss_log_recycle_replication_slot the count of xlog recycle exception by recycle replication slot limit.',
    b'# TYPE opengauss_log_recycle_replication_slot gauge',
    b'opengauss_log_recycle_replication_slot{from_instance="127.0.0.1",lsn="4/8AE62280",'
    b'node_name="cn_5001",replication_slot_limit="4"} 1.0',
    b'# HELP opengauss_log_recycle_build the count of xlog recycle exception by build.',
    b'# TYPE opengauss_log_recycle_build gauge',
    b'opengauss_log_recycle_build{from_instance="127.0.0.1",node_name="cn_5001"} 1.0',
    b'# HELP opengauss_log_recycle_full_build the count of xlog recycle exception by full build.',
    b'# TYPE opengauss_log_recycle_full_build gauge',
    b'opengauss_log_recycle_full_build{from_instance="127.0.0.1",lsn="4/8EE62280",'
    b'node_name="cn_5001",segno="6229"} 1.0',
    b'# HELP opengauss_log_recycle_quorum_required the count of xlog recycle quorum required.',
    b'# TYPE opengauss_log_recycle_quorum_required gauge',
    b'opengauss_log_recycle_quorum_required{from_instance="127.0.0.1",lsn="4/8EF62280",'
    b'lsn_required="4/8FF62280",node_name="cn_5001",segno="6230"} 1.0',
    b'# HELP opengauss_log_recycle_dcf_zero the count of xlog recycle exception by dcf idx zero.',
    b'# TYPE opengauss_log_recycle_dcf_zero gauge',
    b'opengauss_log_recycle_dcf_zero{from_instance="127.0.0.1",lsn="4/8BE62280",'
    b'node_name="cn_5001",segno="6221"} 1.0',
    b'# HELP opengauss_log_recycle_dcf_else the count of xlog recycle exception by dcf for other reason.',
    b'# TYPE opengauss_log_recycle_dcf_else gauge',
    b'opengauss_log_recycle_dcf_else{from_instance="127.0.0.1",node_name="cn_5001",segno="6222"} 1.0',
    b'# HELP opengauss_log_recycle_dummy_standby the count of xlog recycle exception by dummy standby.',
    b'# TYPE opengauss_log_recycle_dummy_standby gauge',
    b'opengauss_log_recycle_dummy_standby{from_instance="127.0.0.1",node_name="cn_5001",segno="6223"} 1.0',
    b'# HELP opengauss_log_recycle_cbm the count of xlog recycle exception by cbm.',
    b'# TYPE opengauss_log_recycle_cbm gauge',
    b'opengauss_log_recycle_cbm{from_instance="127.0.0.1",lsn="4/8CE62280",node_name="cn_5001",segno="6224"} 1.0',
    b'# HELP opengauss_log_recycle_standby_backup the count of xlog recycle exception by standby backup.',
    b'# TYPE opengauss_log_recycle_standby_backup gauge',
    b'opengauss_log_recycle_standby_backup{from_instance="127.0.0.1",node_name="cn_5001",segno="6225"} 1.0',
    b'# HELP opengauss_log_recycle_extro_read_zero the count of xlog recycle exception by extro read.',
    b'# TYPE opengauss_log_recycle_extro_read_zero gauge',
    b'opengauss_log_recycle_extro_read_zero{from_instance="127.0.0.1",node_name="cn_5001",segno="6227"} 1.0',
    b'# HELP opengauss_log_recycle_extro_read_else the count of xlog recycle exception by other extro read.',
    b'# TYPE opengauss_log_recycle_extro_read_else gauge',
    b'opengauss_log_recycle_extro_read_else{from_instance="127.0.0.1",lsn="4/8DE62280",'
    b'node_name="cn_5001",segno="6228"} 1.0',
    b'# HELP opengauss_log_node_restart the count of node restart.',
    b'# TYPE opengauss_log_node_restart gauge',
    b'opengauss_log_node_restart{from_instance="127.0.0.1",role="cn"} 1.0',
    b'opengauss_log_node_restart{from_instance="127.0.0.1",role="gtm"} 1.0',
    b'# HELP opengauss_log_node_start the count of node start.',
    b'# TYPE opengauss_log_node_start gauge',
    b'opengauss_log_node_start{command="/usr/local/core/app/bin/opengauss   --coordinator -D /usr/local/cn>> \\'
    b'"/var/lib/log/Ruby/cm/cm_agent/system_call-current.log\\" 2>&1 &",from_instance="127.0.0.1",role="CN"} 1.0',
    b'opengauss_log_node_start{command="ps -ux | grep 1",from_instance="127.0.0.1",role="GTM"} 1.0',
    b'# HELP opengauss_log_cn_status the count of cn status.',
    b'# TYPE opengauss_log_cn_status gauge',
    b'opengauss_log_cn_status{cn_disk_damage="0",cn_manual_stop="0",cn_nic_down="0",cn_port_conflict="0",'
    b'from_instance="127.0.0.1",port="9080"} 1.0',
    b'opengauss_log_cn_status{cn_disk_damage="0",cn_manual_stop="1",cn_nic_down="0",cn_port_conflict="0",'
    b'from_instance="127.0.0.1",port="9080"} 1.0',
    b'opengauss_log_cn_status{cn_disk_damage="1",cn_manual_stop="0",cn_nic_down="0",cn_port_conflict="0",'
    b'from_instance="127.0.0.1",port="9080"} 1.0',
    b'opengauss_log_cn_status{cn_disk_damage="0",cn_manual_stop="0",cn_nic_down="1",cn_port_conflict="0",'
    b'from_instance="127.0.0.1",port="9080"} 1.0',
    b'opengauss_log_cn_status{cn_disk_damage="0",cn_manual_stop="0",cn_nic_down="0",cn_port_conflict="1",'
    b'from_instance="127.0.0.1",port="9080"} 1.0',
    b'# HELP opengauss_log_cn_disk_status_after_removed the count of cn disk damage after cn is removed.',
    b'# TYPE opengauss_log_cn_disk_status_after_removed gauge',
    b'opengauss_log_cn_disk_status_after_removed{cn_disk_damage_after_removed="1",'
    b'from_instance="127.0.0.1",instance_id="5002"} 1.0',
    b'# HELP opengauss_log_dn_status the count of dn status.',
    b'# TYPE opengauss_log_dn_status gauge',
    b'opengauss_log_dn_status{dn_disk_damage="0",dn_manual_stop="1",dn_nic_down="0",dn_port_conflict="0",'
    b'from_instance="127.0.0.1",instance_id="0"} 1.0',
    b'opengauss_log_dn_status{dn_disk_damage="1",dn_manual_stop="0",dn_nic_down="0",dn_port_conflict="0",'
    b'from_instance="127.0.0.1",instance_id="0"} 1.0',
    b'opengauss_log_dn_status{dn_disk_damage="0",dn_manual_stop="0",dn_nic_down="1",dn_port_conflict="0",'
    b'from_instance="127.0.0.1",instance_id="0"} 1.0',
    b'opengauss_log_dn_status{dn_disk_damage="0",dn_manual_stop="0",dn_nic_down="0",dn_port_conflict="1",'
    b'from_instance="127.0.0.1",instance_id="0"} 1.0',
    b'# HELP opengauss_log_gtm_status the count of gtm status.',
    b'# TYPE opengauss_log_gtm_status gauge',
    b'opengauss_log_gtm_status{from_instance="127.0.0.1",gtm_disk_damage="0",gtm_manual_stop="1",'
    b'gtm_nic_down="0",gtm_port_conflict="0",port="9080"} 1.0',
    b'opengauss_log_gtm_status{from_instance="127.0.0.1",gtm_disk_damage="1",gtm_manual_stop="0",'
    b'gtm_nic_down="0",gtm_port_conflict="0",port="9080"} 1.0',
    b'opengauss_log_gtm_status{from_instance="127.0.0.1",gtm_disk_damage="0",gtm_manual_stop="0",'
    b'gtm_nic_down="1",gtm_port_conflict="0",port="9080"} 1.0',
    b'opengauss_log_gtm_status{from_instance="127.0.0.1",gtm_disk_damage="0",gtm_manual_stop="0",'
    b'gtm_nic_down="0",gtm_port_conflict="1",port="9080"} 1.0',
    b'# HELP opengauss_log_dn_writable_failed the count of dn writable failed.',
    b'# TYPE opengauss_log_dn_writable_failed gauge',
    b'opengauss_log_dn_writable_failed{from_instance="127.0.0.1"} 1.0',
    b'# HELP opengauss_log_cms_heartbeat_timeout_restart the count of cms heartbeat timeout.',
    b'# TYPE opengauss_log_cms_heartbeat_timeout_restart gauge',
    b'opengauss_log_cms_heartbeat_timeout_restart{from_instance="127.0.0.1",instance_id="6001"} 1.0',
    b'# HELP opengauss_log_cms_phonydead_restart the count of cms phony dead times.',
    b'# TYPE opengauss_log_cms_phonydead_restart gauge',
    b'opengauss_log_cms_phonydead_restart{from_instance="127.0.0.1",instance_id="6001"} 1.0',
    b'# HELP opengauss_log_cms_cn_down the count of cms down.',
    b'# TYPE opengauss_log_cms_cn_down gauge',
    b'opengauss_log_cms_cn_down{cn_dn_disconnected="0",cn_down_to_delete="1",cn_tp_net_deleted="None",'
    b'er_delete_cn="None",from_instance="127.0.0.1",instance_id="5001"} 1.0',
    b'opengauss_log_cms_cn_down{cn_dn_disconnected="1",cn_down_to_delete="0",cn_tp_net_deleted="None",'
    b'er_delete_cn="None",from_instance="127.0.0.1",instance_id="5002"} 1.0',
    b'# HELP opengauss_log_cn_restart_time_exceed the count of cn restart time exceed.',
    b'# TYPE opengauss_log_cn_restart_time_exceed gauge',
    b'opengauss_log_cn_restart_time_exceed{from_instance="127.0.0.1"} 1.0',
    b'# HELP opengauss_log_cms_read_only the count of cn read only.',
    b'# TYPE opengauss_log_cms_read_only gauge',
    b'opengauss_log_cms_read_only{from_instance="127.0.0.1",instance_id="6001"} 4.0',
    b'# HELP opengauss_log_cms_restart_pending the count of cms restart pending.',
    b'# TYPE opengauss_log_cms_restart_pending gauge',
    b'opengauss_log_cms_restart_pending{from_instance="127.0.0.1",instance_id="6001"} 1.0',
    b'# HELP opengauss_log_cms_heartbeat_timeout the count of cms heartbeat timeout.',
    b'# TYPE opengauss_log_cms_heartbeat_timeout gauge',
    b'opengauss_log_cms_heartbeat_timeout{from_instance="127.0.0.1",instance_id="6001"} 1.0',
    b'# HELP opengauss_log_bind_ip_failed the count of bind ip failed.',
    b'# TYPE opengauss_log_bind_ip_failed gauge',
    b'opengauss_log_bind_ip_failed{from_instance="127.0.0.1",socket="IPV4"} 1.0',
    b'# HELP opengauss_log_ffic the count of ffic.',
    b'# TYPE opengauss_log_ffic gauge',
    b'opengauss_log_ffic{debug_sql_id="0",from_instance="127.0.0.1",unique_sql_id="0"} 1.0',
    b'# HELP opengauss_log_cn_dn_disconnection the topology disconnection.',
    b'# TYPE opengauss_log_cn_dn_disconnection gauge',
    b'opengauss_log_cn_dn_disconnection{from_instance="127.0.0.1",instance_id="5001",'
    b'node_code="1-2",role="cn"} 1.0',
    b''
]

DISABLED = [
    b'# HELP opengauss_log_flow_control the count of flow control.',
    b'# TYPE opengauss_log_flow_control gauge',
    b'opengauss_log_flow_control{control_type="RPO",datname="[unknown]",'
    b'from_instance="127.0.0.1",host="localhost",node_name="cn_5001"} 1.0',
    b'opengauss_log_flow_control{control_type="RTO",datname="[unknown]",'
    b'from_instance="127.0.0.1",host="localhost",node_name="cn_5001"} 1.0',
    b'# HELP opengauss_log_lock_wait_timeout the count of lock timeout.',
    b'# TYPE opengauss_log_lock_wait_timeout gauge',
    b'opengauss_log_lock_wait_timeout{datname="[unknown]",from_instance="127.0.0.1",'
    b'host="localhost",lockmode="RowExclusiveLock",node_name="cn_5001",'
    b'statement="<insert into houlei values(generate_series(1,2000000));>",'
    b'thread="139940845319936"} 1.0',
    b'# HELP opengauss_log_recycle_auxilary_db the count of xlog recycle exception by auxilary db.',
    b'# TYPE opengauss_log_recycle_auxilary_db gauge',
    b'opengauss_log_recycle_auxilary_db{node_name="cn_5001",segno="0"} 1.0',
    b'# HELP opengauss_log_gtm_disconnected_to_primary the count of gtm disconnected to primary.',
    b'# TYPE opengauss_log_gtm_disconnected_to_primary gauge',
    b'opengauss_log_gtm_disconnected_to_primary{from_instance="127.0.0.1"} 2.0',
    b'# HELP opengauss_log_gtm_panic the count of gtm panic or fatal.',
    b'# TYPE opengauss_log_gtm_panic gauge',
    b'opengauss_log_gtm_panic{from_instance="127.0.0.1"} 2.0',
    b'# HELP opengauss_log_authentication_timeout the count of authentication timeout.',
    b'# TYPE opengauss_log_authentication_timeout gauge',
    b'opengauss_log_authentication_timeout{app_name="dn_6001",datname="[unknown]",from_instance="127.0.0.1",'
    b'host="localhost",node_name="cn_5001",thread_name="BACKEND",user_name="[unknown]"} 1.0',
]

NORMAL_EXPECTED = {
    'cms_state': [
        {'ip': '127.0.0.1', 'path': '/cms', 'state': 'Standby'},
        {'ip': '127.0.0.1', 'path': '/cms', 'state': 'Primary'},
        {'ip': '127.0.0.1', 'path': '/cms', 'state': 'Standby'}
    ],
    'etcd_state': [
        {'ip': '127.0.0.1', 'instance_id': '7001', 'path': '/etcd', 'state': 'StateLeader'},
        {'ip': '127.0.0.1', 'instance_id': '7002', 'path': '/etcd', 'state': 'StateFollower'},
        {'ip': '127.0.0.1', 'instance_id': '7003', 'path': '/etcd', 'state': 'StateFollower'}
    ],
    'cn_state': [
        {'ip': '127.0.0.1', 'instance_id': '5001', 'port': '19080', 'path': '/cn', 'state': 'Deleted'},
        {'ip': '127.0.0.1', 'instance_id': '5002', 'port': '19080', 'path': '/cn', 'state': 'Normal'}
    ],
    'central_cn_state': [
        {'ip': '127.0.0.1', 'instance_id': '5002', 'path': '/central', 'state': 'Normal'}
    ],
    'gtm_state': [
        {'ip': '127.0.0.2', 'instance_id': '1001', 'path': '/gtm', 'role': 'Down', 'state': 'Disk damaged',
         'sync_state': 'Sync'},
        {'ip': '127.0.0.2', 'instance_id': '1002', 'path': '/gtm', 'role': 'Primary', 'state': 'Connection ok',
         'sync_state': 'Sync'},
        {'ip': '127.0.0.2', 'instance_id': '1003', 'path': '/gtm', 'role': 'Standby', 'state': 'Connection ok',
         'sync_state': 'Sync'}
    ],
    'dn_state': [
        {'ip': '127.0.0.1', 'instance_id': '6001', 'port': '9080', 'path': '/dn', 'role': 'Down',
         'state': 'Disk damaged'},
        {'ip': '127.0.0.1', 'instance_id': '6002', 'port': '9080', 'path': '/dn', 'role': 'Primary',
         'state': 'Need repair'},
        {'ip': '127.0.0.1', 'instance_id': '6003', 'port': '9080', 'path': '/dn', 'role': 'Standby',
         'state': 'Normal'}
    ]
}

ERROR_EXPECTED = {
    'cms_state': [
        {'ip': '127.0.0.1', 'path': '/cms', 'state': 'Standby'},
        {'ip': '127.0.0.1', 'path': '/cms', 'state': 'Primary'},
        {'ip': '127.0.0.1', 'path': '/cms', 'state': 'Standby'}
    ],
    'etcd_state': [
        {'ip': '127.0.0.1', 'instance_id': '7001', 'path': '/etcd', 'state': 'StateLeader'},
        {'ip': '127.0.0.1', 'instance_id': '7002', 'path': '/etcd', 'state': 'StateFollower'},
        {'ip': '127.0.0.1', 'instance_id': '7003', 'path': '/etcd', 'state': 'StateFollower'}],
    'cn_state': [
        {'ip': '127.0.0.1', 'instance_id': '5001', 'port': '19080', 'path': '/cn',
         'state': 'abnormal_output_from_cm_ctl_query'},
        {'ip': '127.0.0.1', 'instance_id': '5002', 'port': '19080', 'path': '/cn',
         'state': 'abnormal_output_from_cm_ctl_query'}
    ],
    'central_cn_state': [
        {'ip': '127.0.0.1', 'instance_id': '5002', 'path': '/central', 'state': 'Normal'}
    ],
    'gtm_state': [
        {'ip': '127.0.0.2', 'instance_id': '1001', 'path': '/gtm', 'role': 'Down',
         'state': 'abnormal_output_from_cm_ctl_query',
         'sync_state': 'abnormal_output_from_cm_ctl_query'},
        {'ip': '127.0.0.2', 'instance_id': '1002', 'path': '/gtm', 'role': 'Primary',
         'state': 'Connection ok', 'sync_state': 'Sync'},
        {'ip': '127.0.0.2', 'instance_id': '1003', 'path': '/gtm', 'role': 'Standby',
         'state': 'Connection ok', 'sync_state': 'Sync'}
    ],
    'dn_state': [
        {'ip': '127.0.0.1', 'instance_id': '6001', 'port': '9080', 'path': '/dn', 'role': 'Down',
         'state': 'Disk damaged'},
        {'ip': '127.0.0.1', 'instance_id': '6002', 'port': '9080', 'path': '/dn', 'role': 'Primary',
         'state': 'Need repair'},
        {'ip': '127.0.0.1', 'instance_id': '6003', 'port': '9080', 'path': '/dn', 'role': 'Standby',
         'state': 'Normal'}
    ]
}

COPIED_PERFORM_SHELL_COMMAND = copy.deepcopy(utils.perform_shell_command)

MOCK_CACHE = {
    "opengauss_cluster": {
        "cm_state": CM_CTL_QUERY_CVIDP,
        "state": ['0'],
        'cms_state': [
            ('[{"ip": "127.0.0.1", "path": "/cms", "state": "Standby"}, '
             '{"ip": "127.0.0.1", "path": "/cms", "state": "Primary"}, '
             '{"ip": "127.0.0.1", "path": "/cms", "state": "Standby"}]')
        ],
        'etcd_state': [
            ('[{"ip": "127.0.0.1", "instance_id": "7001", "path": "/etcd", "state": "StateLeader"}, '
             '{"ip": "127.0.0.1", "instance_id": "7002", "path": "/etcd", "state": "StateFollower"}, '
             '{"ip": "127.0.0.1", "instance_id": "7003", "path": "/etcd", "state": "StateFollower"}]')
        ],
        'cn_state': [
            ('[{"ip": "127.0.0.1", "instance_id": "5001", "port": "19080", "path": "/cn", "state": "Deleted"}, '
             '{"ip": "127.0.0.1", "instance_id": "5002", "port": "19080", "path": "/cn", "state": "Normal"}]')
        ],
        'central_cn_state': [
            '[{"ip": "127.0.0.1", "instance_id": "5002", "path": "/central", "state": "Normal"}]'
        ],
        'gtm_state': [
            ('[{"ip": "127.0.0.2", "instance_id": "1001", "path": "/gtm", "role": "Down", '
             '"state": "Disk damaged", "sync_state": "Sync"}, '
             '{"ip": "127.0.0.2", "instance_id": "1002", "path": "/gtm", "role": "Primary", '
             '"state": "Connection ok", "sync_state": "Sync"}, '
             '{"ip": "127.0.0.2", "instance_id": "1003", "path": "/gtm", "role": "Standby", '
             '"state": "Connection ok", "sync_state": "Sync"}]')
        ],
        'dn_state': [
            ('[{"ip": "127.0.0.1", "instance_id": "6001", "port": "9080", "path": "/dn", '
             '"role": "Down", "state": "Disk damaged"}, '
             '{"ip": "127.0.0.1", "instance_id": "6002", "port": "9080", "path": "/dn", '
             '"role": "Primary", "state": "Need repair"}, '
             '{"ip": "127.0.0.1", "instance_id": "6003", "port": "9080", "path": "/dn", '
             '"role": "Standby", "state": "Normal"}]')
        ],
        'primary': ['127.0.0.1:9080'],
        'standby': ['127.0.0.1:9080'],
        'normal': ['127.0.0.1:9080'],
        'abnormal': ['127.0.0.1:9080,127.0.0.1:9080']
    },
    'opengauss_process': {
        'process_state': {
            'cpu_usage': ['99.5', '0.0', '0.0', '0.0'],
            'mem_usage': ['3.4', '0.0', '0.0', '0.0'],
            'leaked_fds': ['0', '0', '0', '0'],
            'user': ['cent', 'cent', 'cent', 'cent'],
            'role': ['cn', 'dn', 'dn', 'dn'],
            'pid': ['11', '22', '33', '44'],
            'ip': ['127.0.0.1', '127.0.0.1', '127.0.0.1', '127.0.0.1'],
            'port': ['19080', '9080', '9080', '9080'],
            'cwd': ['/home/1', '/home/2', '/home/3', '/home/4']
        },
        'cpu_usage': ['99.5', '0.0', '0.0', '0.0'],
        'mem_usage': ['3.4', '0.0', '0.0', '0.0'],
        'user': ['cent', 'cent', 'cent', 'cent'],
        'role': ['cn', 'dn', 'dn', 'dn'],
        'pid': ['11', '22', '33', '44'],
        'ip': ['127.0.0.1', '127.0.0.1', '127.0.0.1', '127.0.0.1'],
        'port': ['19080', '9080', '9080', '9080'],
        'cwd': ['/home/1', '/home/2', '/home/3', '/home/4']
    },
    'opengauss_ping': {
        'ip_info': {
            ('127.0.0.1', '192.168.0.2'): ('cn,cms', 'dn,gtm', True),
        },
        'ping_state': {
            'state': ['1'],
            'packet_rate': ['1'],
            'lag': ['1'],
            'source': ['127.0.0.1'],
            'target': ['192.168.0.2'],
            'to_primary_dn': [True],
        }
    },
    'opengauss_mount': {
        'local_ips': ['127.0.0.1', '127.0.0.1'],
        'blk_info': {
            'vda': [('vda2', ''),
                    ('vda1', '/boot'),
                    ('dm-0', '/'),
                    ('dm-1', '/tmp'),
                    ('vda3', '')],
            'vdb': [('vdb3', '/var/chroot/var/lib/engine/data1'),
                    ('vdb2', '/var/chroot/usr/local'),
                    ('vdb1', '/var/chroot/var/lib/log')],
        },
        'df_info': {
            '/dev/mapper/VolGroup-lv_root': {('32%', '/')},
            'devtmpfs': {('0%', '/dev')},
            'tmpfs': {('1%', '/dev/shm'), ('0%', '/sys/fs/cgroup'), ('0%', '/run/user/1000'),
                      ('1%', '/run'), ('0%', '/run/user/0')},
            '/dev/vda1': {('6%', '/boot')},
            '/dev/mapper/VolGroup-lv_tmp': {('1%', '/tmp')},
            '/dev/vdb2': {('10%', '/var/chroot/usr/local')},
            '/dev/vdb1': {('2%', '/var/chroot/var/lib/log')},
            '/dev/vdb3': {('3%', '/var/chroot/var/lib/engine/data1')},
            '127.0.0.1:/usr1/mnt/data': {('82%', '/mnt/data')}
        },
        "mount_state": {
            "usage": ["32", "32", "32", "32", "32"],
            "role": ["cn", "cn", "dn", "dn", "dn"],
            "ip": ["127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1"],
            "port": ["5001", "5002", "6001", "6002", "6003"],
            "path": ["/cn", "/cn", "/dn", "/dn", "/dn"],
            "device": ["vda", "vda", "vda", "vda", "vda"],
            "file_system": ["/dev/mapper/VolGroup-lv_root", "/dev/mapper/VolGroup-lv_root",
                            "/dev/mapper/VolGroup-lv_root", "/dev/mapper/VolGroup-lv_root",
                            "/dev/mapper/VolGroup-lv_root"],
            "kernel_name": ["dm-0", "dm-0", "dm-0", "dm-0", "dm-0"],
            "instance_id": ["6001", "6001", "6001"],
        }
    },
    'opengauss_xlog': {
        'xlog_state': {
            'count': ['0', '0', '0'],
            'oldest_lsn': ['0', '0', '0'],
            'role': ['dn', 'dn', 'dn'],
            'path': ['/dn', '/dn', '/dn'],
            'from_instance': ['127.0.0.1:9080', '127.0.0.1:9080', '127.0.0.1:9080'],
            "device": ["vda", "vda", "vda"],
            "instance_id": ["6001", "6001", "6001"],
        }
    },
    'opengauss_nic': {
        'ip': ['["127.0.0.1", "127.0.0.1"]'],
        'state': ['1']
    }
}

CM_CTL_VIEW = "datanodeXlogPath :"


def mock_process_args(args, remaining_time):
    res = dict()
    if args is None:
        return res

    for arg_name, value in args.items():
        if arg_name == "timeout" and value == "remaining_time":
            res[arg_name] = remaining_time
        elif "CACHE:" in value:
            _, name, label = value.split(":")
            if label == "ALL":
                res[arg_name] = MOCK_CACHE.get(name)
            else:
                res[arg_name] = MOCK_CACHE.get(name).get(label)
        else:
            res[arg_name] = value

    return res


def mock_perform_shell_command(cmd, stdin, timeout=None):
    outputs = {
        "cm_ctl query": (0, CM_CTL_QUERY_CVIDP),
        "ps": (0, PS_UX),
        "lsof": (0, "opengauss 21317  omm"),
        "ss": (0, 'tcp LISTEN 0 6144 *:* *:* users:(("opengauss",pid=21317,fd=1))'),
        "readlink": (0, "/home/123"),
        "timeout": (0, "0% packet loss"),
        "lsblk": (0, LSBLK),
        "df": (0, DF),
        "hostname": (0, "127.0.0.1 127.0.0.1"),
        "cm_ctl view": (0, CM_CTL_VIEW),
    }
    for head, output in outputs.items():
        if cmd.startswith(head):
            return output

    return COPIED_PERFORM_SHELL_COMMAND(cmd, stdin, timeout)


class Event:
    def __init__(self, header, body):
        self.body = body
        self.header = header


class MockTaildirSource:
    def __init__(self, *args, **kwargs):
        kwargs = {
            "date": datetime.now().strftime('%Y-%m-%d'),
            "time": datetime.now().strftime('%H:%M:%S.%f'),
            "ts": int(datetime.now().timestamp() * 1000)
        }
        self.channel = queue.Queue()
        for path, lines in TEST_LOG_LINES.items():
            for line in lines:
                header = {"path": path, "type": IN_MODIFY}
                self.channel.put(Event(header, line.format(**kwargs)))

    def start(self):
        pass

    def stop(self):
        pass


def test_cmd_exporter(monkeypatch):
    if not LINUX:
        return

    parser = argparse.ArgumentParser()
    parser.add_argument('--web.listen-address', default="127.0.0.1")
    parser.add_argument('--web.listen-port', default="1234")
    parser.add_argument('--disable-https', action='store_true', default=True)
    parser.add_argument('--pg-log-dir', default="/")
    parser.add_argument('--log.filepath', default=LOG_PATH)
    parser.add_argument('--log.level', default='info')
    parser.add_argument('--parallel', default=5)
    parser.add_argument('--config', default=YAML_PATH)
    parser.add_argument('--ssl-keyfile')
    parser.add_argument('--ssl-certfile')
    parser.add_argument('--ssl-ca-file')
    parser.add_argument('--keyfile-password')
    parser.add_argument('--constant-labels', default='')
    args = parser.parse_args([])

    monkeypatch.setattr(controller, 'run', mock.MagicMock())
    monkeypatch.setattr(impl, 'process_args', mock_process_args)
    monkeypatch.setattr(utils, 'perform_shell_command', mock_perform_shell_command)
    monkeypatch.setattr(log_extractor, 'TaildirSource', MockTaildirSource)
    monkeypatch.setattr(constants, 'GSQL_V', "gsql (GaussDB Kernel 505.1.0 build)")

    ExporterMain(args).run()

    assert service.block_query_all_metrics().split(b'\n') == EXPECTED


def test_cm_ctl_parse():
    if not LINUX:
        return

    with open(YAML_PATH, errors='ignore') as fp:
        cmd_yml = yaml.safe_load(fp)

    normal_results = dict()
    error_results = dict()
    for metric in cmd_yml.get("opengauss_cluster").get("metrics"):
        name, usage = metric.get("name"), metric.get("usage")
        if name.endswith("state") and usage is not None:
            subquery = metric.get("args").get("cmd")
            normal_result = utils.parse_state_detail(name, subquery, CM_CTL_QUERY_CVIDP, 10)
            if normal_result:
                normal_status = json.loads(normal_result[0])
                normal_results[name] = normal_status

            error_result = utils.parse_state_detail(name, subquery, ERR_CM_CTL_QUERY_CVIDP, 10)
            if error_result:
                error_status = json.loads(error_result[0])
                error_results[name] = error_status

    assert normal_results == NORMAL_EXPECTED
    assert error_results == ERROR_EXPECTED
