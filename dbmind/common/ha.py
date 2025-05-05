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
import multiprocessing
import os

from dbmind.common.types.enums import ServiceState, CheckErrorMsg, RepairErrorMsg, ProcessSuggest
from dbmind.metadatabase.result_db_session import get_session
from dbmind.metadatabase.schema import RegularInspection

MAX_MEM_USAGE = 1.0
MAX_MEM_SIZE = 200 # MB
max_cpu_usage_dict = {
    'cmd_exporter': 10.0,
    'opengauss_exporter': 10.0,
    'reprocessing_exporter': 10.0,
    'DBMind': 80.0
}
max_cpu_usage_single_dict = {
    'cmd_exporter': 50.0,
    'opengauss_exporter': 50.0,
    'reprocessing_exporter': 50.0
}
abnormal_count_dict = {
    'cur_count': 0,
    'max_count': 3,
    'cpu_count': 0
}

CONSTANT_LABELS_INSTANCE_LENGTH = 2
STATM_TOTAL_NUM = 7
RESIDUAL_PAGE_INDEX = 1
SHARED_PAGE_INDEX = 2
MB_KB_RATE = 1024
PERCENTAGE = 100
STAT_TOTAL_NUM = 52
UPTIME_TOTAL_NUM = 2
UTIME_INDEX = 13
STIME_INDEX = 14
START_TIME_INDEX = 21
# the k-v of prev_time_dict is used to record cpu usage information and cannot be used for other purposes.
prev_time_dict = {
    'utime': 0,
    'stime': 0,
    'start_time': 0
}


def check_param_validity(cmd, component_type):
    if not cmd.strip():
        return False
    if component_type not in cmd:
        return False
    param_list = cmd.split(' ')
    while '' in param_list:
        param_list.remove('')
    param_set = set(param_list)
    if len(param_set) < len(param_list):
        return False
    return True


def get_pid_file_constant_labels(cmd, proj_path, pid_filename, component_type, constant_labels_instance):
    if 'constant-labels' not in cmd:
        if constant_labels_instance:
            return ''
        pid_file = os.path.join(proj_path, pid_filename)
        return pid_file
    args = cmd.split(" ")
    for argv in args:
        if not argv.startswith('instance'):
            continue
        instance_list = argv.split("=")
        if len(instance_list) != CONSTANT_LABELS_INSTANCE_LENGTH:
            break
        if instance_list[1] != constant_labels_instance:
            break
        pid_file = os.path.join(proj_path, '{}_{}.pid'.format(component_type, instance_list[1]))
        return pid_file
    return ''


def get_log_path(cmd, log_path):
    if 'log.filepath' not in cmd:
        return log_path
    args = cmd.split(" ")
    while '' in args:
        args.remove('')
    for index, argv in enumerate(args):
        if argv != '--log.filepath':
            continue
        if index < len(args) - 1 and args[index + 1] == log_path:
            return args[index + 1]
    return ''


def get_self_mem_usage():
    try:
        with open("/proc/self/statm", "r") as f:
            data = f.readline().split()
        if len(data) != STATM_TOTAL_NUM:
            logging.warning('%s, because the data is incorrect.', CheckErrorMsg.MEM_USAGE_INVALID.value)
            return False, CheckErrorMsg.MEM_USAGE_INVALID.value, 0.0
        resident_page = int(data[RESIDUAL_PAGE_INDEX])
        shared_page = int(data[SHARED_PAGE_INDEX])
        used_mem = (resident_page - shared_page) * os.sysconf('SC_PAGE_SIZE') # B
        total_mem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') # B
        if total_mem == 0:
            logging.warning('%s, because the total memory is zero.', CheckErrorMsg.MEM_USAGE_INVALID.value)
            return False, CheckErrorMsg.MEM_USAGE_INVALID.value, 0.0
        mem_usage_rate = used_mem / total_mem * PERCENTAGE
        if used_mem < MAX_MEM_SIZE * MB_KB_RATE * MB_KB_RATE:
            mem_usage_rate = 0.0
    except Exception as e:
        logging.warning('%s, because exception occurred: %s', CheckErrorMsg.MEM_USAGE_INVALID.value, str(e))
        return False, CheckErrorMsg.MEM_USAGE_INVALID.value, 0.0
    return True, '', mem_usage_rate


def get_self_cpu_usage():
    # this method is not idempotent, cpu usage is calculated from the last execution of this function.
    # So we use prev_time_dict to record global information and update every execution.
    try:
        with open('/proc/self/stat', "r") as f:
            data = f.readline().split()
        with open("/proc/uptime", "r") as f:
            time_list = f.readline().split()
        if len(data) < STAT_TOTAL_NUM or len(time_list) != UPTIME_TOTAL_NUM:
            logging.warning('%s, because the data is incorrect.', CheckErrorMsg.CPU_USAGE_INVALID.value)
            return True, CheckErrorMsg.CPU_USAGE_INVALID.value, 0.0
        # the process name of DBMind is (DBMind [Master ), which contains three spaces.
        index_offset = len(data) - STAT_TOTAL_NUM
        cpu_count = multiprocessing.cpu_count()
        clock_hz = float(os.sysconf(os.sysconf_names['SC_CLK_TCK']))
        if clock_hz == 0:
            logging.warning('%s, because the clock_hz of system is zero.', CheckErrorMsg.CPU_USAGE_INVALID.value)
            return True, CheckErrorMsg.CPU_USAGE_INVALID.value, 0.0
        uptime = float(time_list[0])
        utime = int(data[UTIME_INDEX + index_offset]) / clock_hz
        stime = int(data[STIME_INDEX + index_offset]) / clock_hz
        if prev_time_dict['start_time'] == 0:
            prev_time_dict['start_time'] = int(data[START_TIME_INDEX + index_offset]) / clock_hz
        runtime = uptime - prev_time_dict['start_time']
        utime_cost = utime - prev_time_dict['utime']
        stime_cost = stime - prev_time_dict['stime']
        cpu_time_cost = utime_cost + stime_cost
        if runtime == 0 or cpu_count == 0:
            logging.warning('%s, because the interval between two calls is zero.',
                            CheckErrorMsg.CPU_USAGE_INVALID.value)
            return True, CheckErrorMsg.CPU_USAGE_INVALID.value, 0.0
        cpu_usage_rate = PERCENTAGE * cpu_time_cost / runtime / cpu_count
        abnormal_count_dict['cpu_count'] = cpu_count
        # The content in prev_time_dict is global information, ensure that no other function modifies its value.
        prev_time_dict['utime'] = utime
        prev_time_dict['stime'] = stime
        prev_time_dict['start_time'] = uptime
    except Exception as e:
        logging.warning('%s, because exception occurred: %s', CheckErrorMsg.MEM_USAGE_INVALID.value, str(e))
        return False, CheckErrorMsg.CPU_USAGE_INVALID.value, 0.0
    return True, '', cpu_usage_rate


def get_self_resource_usage():
    cpu_status, cpu_msg, cpu_usage_rate = get_self_cpu_usage()
    if not cpu_status:
        return cpu_status, cpu_msg, cpu_usage_rate, 0.0
    mem_status, mem_msg, mem_usage_rate = get_self_mem_usage()
    if not mem_status:
        return mem_status, mem_msg, 0.0, mem_usage_rate
    return True, 'success', cpu_usage_rate, mem_usage_rate


def check_log_file(log_path):
    logging.info('check log file validity')
    if not log_path:
        logging.warning('log_path is None')
        return False, {'state': ServiceState.FAIL.value, 'error_msg': CheckErrorMsg.CMD_INCORRECT.value,
                       'result': {"suggest": ProcessSuggest.RESTART_PROCESS.value}}
    if os.path.isdir(log_path) or not os.path.exists(log_path):
        logging.warning('log_path is not exist.')
        return False, {'state': ServiceState.FAIL.value, 'error_msg': CheckErrorMsg.LOG_FILE_MISSING.value,
                       'result': {"suggest": ProcessSuggest.RESTART_PROCESS.value}}
    return True, {}


def check_pid_file(pid_file):
    try:
        if not pid_file:
            logging.warning('pid_file is None')
            return False, {'state': ServiceState.FAIL.value, 'error_msg': CheckErrorMsg.CMD_INCORRECT.value,
                           'result': {"suggest": ProcessSuggest.RESTART_PROCESS.value}}
        if os.path.isdir(pid_file) or not os.path.exists(pid_file):
            os.makedirs(os.path.dirname(pid_file), mode=0o700, exist_ok=True)
            with open(pid_file, 'w+') as fp:
                fp.write('%d\n' % os.getpid())
            os.chmod(pid_file, 0o600)
            logging.info('create pid file')
            return True, {}
        with open(pid_file, 'r', encoding="utf-8") as f:
            check_pid = f.read().strip()
        if not check_pid.isdigit() or check_pid != str(os.getpid()):
            os.makedirs(os.path.dirname(pid_file), mode=0o700, exist_ok=True)
            with open(pid_file, 'w+') as fp:
                fp.write('%d\n' % os.getpid())
            os.chmod(pid_file, 0o600)
            logging.info('fix pid content')
            return True, {}
    except Exception as e:
        logging.warning('%s, because exception occurred: %s', CheckErrorMsg.PID_FILE_INVALID.value, str(e))
        return False, {'state': ServiceState.FAIL.value, 'error_msg': CheckErrorMsg.PERMISSION_DENIED.value,
                       'result': {"suggest": ProcessSuggest.RESTART_PROCESS.value}}
    return True, {}


def check_resource_usage(service_type):
    res_usage_status, res_usage_content, cpu_usage, mem_usage = get_self_resource_usage()
    if not res_usage_status:
        return False, {'state': ServiceState.FAIL.value, 'error_msg': res_usage_content,
                       'result': {"suggest": ProcessSuggest.RESTART_PROCESS.value}}
    if service_type not in max_cpu_usage_dict.keys():
        logging.warning('service_type is not in max_cpu_usage_dict.keys(), service_type')
        return True, {}
    single_flag = False
    if service_type in max_cpu_usage_single_dict.keys() and abnormal_count_dict['cpu_count']:
        single_cpu_usage = cpu_usage * abnormal_count_dict['cpu_count']
        if single_cpu_usage > max_cpu_usage_single_dict[service_type]:
            single_flag = True
    abnormal_count_dict['cpu_count'] = 0
    if cpu_usage > max_cpu_usage_dict[service_type] or mem_usage > MAX_MEM_USAGE or single_flag:
        abnormal_count_dict['cur_count'] += 1
    else:
        abnormal_count_dict['cur_count'] = 0
    if abnormal_count_dict['cur_count'] >= abnormal_count_dict['max_count']:
        return False, {'state': ServiceState.FAIL.value,
                       'error_msg': 'resouce usage too high, cpu: {}%, mem: {}%.'.format(cpu_usage, mem_usage),
                       'result': {"suggest": ProcessSuggest.RESTART_PROCESS.value}}
    return True, {}


def check_metadatabase():
    try:
        with get_session() as session:
            _ = session.query(RegularInspection).count()
        return True, {}
    except Exception as e:
        logging.warning('%s, because exception occurred: %s', CheckErrorMsg.METADATABASE_INVALID.value, str(e))
        return False, {'state': ServiceState.FAIL.value, 'error_msg': CheckErrorMsg.METADATABASE_INVALID.value,
                       'result': {"suggest": ProcessSuggest.RESTART_DATABASE.value}}
    return True, {}


def check_database(driver):
    error_info_dict = {'state': ServiceState.FAIL.value, 'error_msg': CheckErrorMsg.DATABASE_INVALID.value,
                       'result': {"suggest": ProcessSuggest.RESTART_PROCESS.value}}
    if not driver:
        return False, error_info_dict
    try:
        driver.is_standby()
        return True, {}
    except Exception as e:
        logging.warning('%s, because exception occurred: %s', CheckErrorMsg.DATABASE_INVALID.value, str(e))
        return False, error_info_dict
    return True, {}


def repair_pid_file(pid_file):
    try:
        if not pid_file:
            return False, {'state': ServiceState.FAIL.value, 'error_msg': RepairErrorMsg.CMD_INCORRECT.value,
                           'result': {"suggest": ProcessSuggest.RESTART_PROCESS.value}}
        if os.path.isdir(pid_file) or not os.path.exists(pid_file):
            os.makedirs(os.path.dirname(pid_file), mode=0o700, exist_ok=True)
            with open(pid_file, 'w+') as fp:
                fp.write('%d\n' % os.getpid())
            os.chmod(pid_file, 0o600)
            return False, {'state': ServiceState.SUCCESS.value, 'error_msg': RepairErrorMsg.PID_FILE_MISSING.value,
                           'result': {}}
        with open(pid_file, 'r', encoding="utf-8") as f:
            check_pid = f.read().strip()
        if not check_pid.isdigit() or check_pid != str(os.getpid()):
            os.makedirs(os.path.dirname(pid_file), mode=0o700, exist_ok=True)
            with open(pid_file, 'w+') as fp:
                fp.write('%d\n' % os.getpid())
            os.chmod(pid_file, 0o600)
            return False, {'state': ServiceState.SUCCESS.value, 'error_msg': RepairErrorMsg.PID_FILE_WRONG.value,
                           'result': {}}
    except (FileNotFoundError, PermissionError):
        return False, {'state': ServiceState.FAIL.value, 'error_msg': CheckErrorMsg.PERMISSION_DENIED.value,
                       'result': {"suggest": ProcessSuggest.RESTART_PROCESS.value}}
    return True, {}


def record_interface_info(interface_type, return_info):
    if 'state' not in return_info.keys():
        logging.error('The return value of %s interface is not correct, return info: %s', interface_type,
                      str(return_info))
    elif return_info['state'] in [ServiceState.FAIL.value, ServiceState.ABNORMAL.value]:
        logging.error('%s interface return info: %s', interface_type, str(return_info))
    elif return_info['state'] in [ServiceState.SUCCESS.value, ServiceState.NORMAL.value]:
        logging.info('%s interface return info: %s', interface_type, str(return_info))
    else:
        logging.warning('%s interface return info: %s', interface_type, str(return_info))


def check_status_common(log_path, pid_file, service_type):
    log_status, log_info = check_log_file(log_path)
    if not log_status:
        return log_status, log_info
    pid_status, pid_info = check_pid_file(pid_file)
    if not pid_status:
        return pid_status, pid_info
    res_usage_status, res_usage_info = check_resource_usage(service_type)
    if not res_usage_status:
        return res_usage_status, res_usage_info
    return True, {}


def check_status_impl(log_path, pid_file, service_type, service_params):
    common_status, common_info = check_status_common(log_path, pid_file, service_type)
    if not common_status:
        return common_info
    # individual check of each exporter
    if service_type == 'opengauss_exporter':
        driver = service_params[0]
        database_status, database_info = check_database(driver)
        if not database_status:
            return database_info
    elif service_type == 'DBMind':
        metadatabase_status, metadatabase_info = check_metadatabase()
        if not metadatabase_status:
            return metadatabase_info
    return {'state': ServiceState.NORMAL.value, 'error_msg': '', 'result': {}}


def repair_interface_common(log_path, pid_file, service_type):
    log_status, log_info = check_log_file(log_path)
    if not log_status:
        return log_status, log_info
    pid_status, pid_info = repair_pid_file(pid_file)
    if not pid_status:
        return pid_status, pid_info
    res_usage_status, res_usage_info = check_resource_usage(service_type)
    if not res_usage_status:
        return res_usage_status, res_usage_info
    return True, {}


def repair_interface_impl(log_path, pid_file, service_type, service_params):
    common_status, common_info = repair_interface_common(log_path, pid_file, service_type)
    if not common_status:
        return common_info
    # individual repair of each exporter
    if service_type == 'opengauss_exporter':
        driver = service_params[0]
        database_status, database_info = check_database(driver)
        if not database_status:
            return database_info
    elif service_type == 'DBMind':
        metadatabase_status, metadatabase_info = check_metadatabase()
        if not metadatabase_status:
            return metadatabase_info
    return {'state': ServiceState.SUCCESS.value, 'error_msg': '', 'result': {}}
