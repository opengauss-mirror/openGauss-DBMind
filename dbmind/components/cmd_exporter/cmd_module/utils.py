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

import concurrent.futures
import json
import logging
import os
import re
import subprocess
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from subprocess import getoutput
import shlex

from dbmind.common.cmd_executor import multiple_cmd_exec
from dbmind.common.utils.checking import IPV6_PATTERN, prepare_ip
from dbmind.components.cmd_exporter.cmd_module.cm_states import CLUSTER_ROLES, PARSE_METHODS

PING_TIMEOUT = 2
N_THREADS = 10
MAX_THREADS = 512


def perform_shell_command(cmd, stdin, timeout=None):
    """
    Perform a shell command by using subprocess module.
    - param cmd: the cmd to be executed.
    - param stdin: the stdin to be passed to the cmd.
    - param timeout: waiting seconds for the command's execution.
    """
    try:
        if isinstance(stdin, str):
            stdin = stdin.encode(errors='ignore')

        _, output, err = multiple_cmd_exec(cmd, input=stdin, timeout=timeout)
        exitcode = 0
    except subprocess.CalledProcessError as ex:
        exitcode, output = ex.returncode, ex.output
    except subprocess.TimeoutExpired:
        logging.warning('Timed out after %d seconds while executing %s, input is %s.',
                        timeout, cmd, stdin)
        exitcode, output = -1, b''
    except FileNotFoundError:
        logging.error('%s with input %s not found.', cmd, stdin)
        exitcode, output = -1, b''
    except PermissionError:
        logging.error('Permission denied: %s with %s.', cmd, stdin)
        exitcode, output = -1, b''
    except Exception as e:
        logging.error('%s raised while executing %s with input %s.', e, cmd, stdin)
        raise e

    if output[-1:] == b'\n':
        output = output[:-1]

    output = output.decode(errors='ignore')

    return exitcode, output


def get_shell_output(cmd, stdin, timeout=None):
    """
    To get the bulk output of perform_shell_command, the purpose of this function
    is to uniform the output for all the processing functions
    - param cmd: the cmd to be executed.
    - param stdin: the stdin to be passed to the cmd.
    - param timeout: waiting seconds for the command's execution.
    """
    return perform_shell_command(cmd, stdin, timeout)[1]


def get_shell_output_lines(cmd, stdin, timeout=None):
    """
    To get the output of perform_shell_command in lines splited by '\n',
    the purpose of this function is to uniform the output for all
    the processing functions
    - param cmd: the cmd to be executed.
    - param stdin: the stdin to be passed to the cmd.
    - param timeout: waiting seconds for the command's execution.
    """
    return perform_shell_command(cmd, stdin, timeout)[1].split('\n')


def get_process_state(cluster_state, local_ips, timeout=None):
    """
    To get the process state of opengauss process
    - param cluster_state: the cluster_state dict.
    - param local_ips: the return list from get_local_ips.
    - param timeout: waiting seconds for the command's execution.
    """

    def get_proc_info(node_role, node_ip, node_port, end_time):
        pid_cmd = f"ss -tulnp | grep {shlex.quote(str(node_port))}"
        remaining_time = None if end_time is None else end_time - time.monotonic()
        pid_output = get_shell_output_lines(pid_cmd, "", timeout=remaining_time)
        if not pid_output:
            return

        pid_list = set()
        listen_pid = set()

        for proc in pid_output:
            info = proc.split()
            if not info:
                break

            search_pid = re.search('.*,pid=(.*?),.*', info[-1])
            if search_pid is None:
                return

            pid = search_pid.groups()[0]
            if "gaussdb" in info[-1]:
                pid_list.add(pid)
                if 'LISTEN' in info:
                    listen_pid.add(pid)

        if not pid_list:
            return

        if listen_pid:
            pid = list(listen_pid)[0]
        else:
            pid = list(pid_list)[0]

        readlink_cmd = f"readlink /proc/{shlex.quote(str(pid))}/cwd | grep -v USER"
        remaining_time = None if end_time is None else end_time - time.monotonic()
        cwd = get_shell_output_lines(readlink_cmd, "", timeout=remaining_time)[0]

        leaked_fds_cmd = f"ls -l /proc/{shlex.quote(str(pid))}/fd | grep '(deleted)' | wc -l"
        remaining_time = None if end_time is None else end_time - time.monotonic()
        leaked_fds = get_shell_output_lines(leaked_fds_cmd, "", timeout=remaining_time)[0]

        ps_cmd = f"ps -u --pid {shlex.quote(str(pid))} | grep -v USER"
        remaining_time = None if end_time is None else end_time - time.monotonic()
        ps_output = get_shell_output_lines(ps_cmd, "", timeout=remaining_time)[0].split()
        if not ps_output:
            return

        cpu_usage, mem_usage, user = ps_output[2], ps_output[3], ps_output[0]

        process_state["cpu_usage"].append(cpu_usage)
        process_state["mem_usage"].append(mem_usage)
        process_state["leaked_fds"].append(leaked_fds)
        process_state["user"].append(user)
        process_state["role"].append(node_role[:-6])
        process_state["pid"].append(pid)
        process_state["ip"].append(node_ip)
        process_state["port"].append(node_port)
        process_state["cwd"].append(cwd)

    endtime = time.monotonic() + timeout if isinstance(timeout, (int, float)) else None
    process_state = defaultdict(list)
    for role, state in cluster_state.items():
        if role not in CLUSTER_ROLES:
            continue

        try:
            real_state = json.loads(state[0])
        except json.decoder.JSONDecodeError:
            continue

        for line in real_state:
            ip, port = line.get("ip"), line.get("port")
            if not port or ip not in local_ips:
                continue

            get_proc_info(role, ip, port, endtime)

    return process_state


def get_ps_info(name, process_state):
    """
    To get the very column from process_state
    - param name: the column name.
    - param process_state: the process_state dict.
    """
    if not process_state:
        return []

    return process_state.get(name, [])


def parse_state_detail(name, cmd, stdin, timeout=None):
    """
    To parse the very cluster role from the bulk output of 'cm_ctl query -Cvidp'.
    - param name: the type-name of cluster node to be parsed.
    - param cmd: the cmd to be executed.
    - param stdin: the stdin to be passed to the cmd.
    - param timeout: waiting seconds for the command's execution.
    """
    parsed_state = get_shell_output(cmd, stdin, timeout)
    method = PARSE_METHODS.get(name)
    res = list()
    if name not in CLUSTER_ROLES:
        return res

    for line in parsed_state.split(","):
        if not line.strip():
            continue

        parsed_line = method(line)
        if isinstance(parsed_line, list):
            res.extend(parsed_line)
        else:
            res.append(parsed_line)

    if res:
        return [json.dumps(res)]
    else:
        return [""]


def parse_dn_state(name, dn_state):
    """
    To parse the normal and abnormal, the primary and standby from dn states.
    - param name: the type-name of cluster node to be parsed.
    - param dn_state: the dn state output.
    """
    res = defaultdict(list)

    try:
        real_state = json.loads(dn_state[0])
    except json.decoder.JSONDecodeError:
        return []

    for line in real_state:
        if line.get("role") == "Primary":
            res["primary"].append(f"{prepare_ip(line.get('ip'))}:{line.get('port')}")
        elif line.get("role") == "Standby":
            res["standby"].append(f"{prepare_ip(line.get('ip'))}:{line.get('port')}")

        if line.get("state") == "Normal":
            res["normal"].append(f"{prepare_ip(line.get('ip'))}:{line.get('port')}")
        else:
            res["abnormal"].append(f"{prepare_ip(line.get('ip'))}:{line.get('port')}")

    return [','.join(res.get(name, []))]


def parse_ip_info(local_ips, cluster_state):
    """
    To parse ip list from cluster state
    - param cluster_state: the cluster_state dict.
    - param local_ips: the return list from get_local_ips.
    """

    source_role = defaultdict(set)
    target_role = defaultdict(set)
    source_info = dict()
    target_info = dict()
    for role_state, state in cluster_state.items():
        if role_state not in CLUSTER_ROLES:
            continue

        try:
            real_state = json.loads(state[0])
        except json.decoder.JSONDecodeError:
            continue

        role = role_state[:-6] if role_state.endswith("_state") else role_state
        for line in real_state:
            ip = line.get("ip")
            if not ip:
                continue

            primary_dn = role == "dn" and line.get("role") == "Primary"
            if ip in local_ips:
                source_role[ip].add(role)
                source_info[ip] = source_info.get(ip) or primary_dn
            else:
                target_role[ip].add(role)
                target_info[ip] = target_info.get(ip) or primary_dn

    ip_info = dict()
    for source_ip, source_roles in source_role.items():
        for target_ip, target_roles in target_role.items():
            if (
                source_roles & target_roles or
                (source_roles & {"cn", "dn", "gtm"} and target_roles & {"cn", "dn", "gtm"})
            ):
                to_primary_dn = bool(source_info.get(source_ip) or target_info.get(target_ip))
                ip_info[(source_ip, target_ip)] = to_primary_dn

    return ip_info


def get_ping_state(ip_info):
    """
    To parse ip state through `ping` command
    - param ip_info: the ip info to ping.
    """

    def get_ping(source, target):
        if IPV6_PATTERN.match(target):
            command = f"ping6 -I {shlex.quote(str(source))} {shlex.quote(str(target))} -c 1 -w {PING_TIMEOUT}"
        else:
            command = f"ping -I {shlex.quote(str(source))} {shlex.quote(str(target))} -c 1 -w {PING_TIMEOUT}"

        lag_pattern = re.compile("time=(.*?) ms")
        try:
            res = getoutput(cmd=command)
            connected = '0% packet loss' in res and '100% packet loss' not in res
            net_lag = float(lag_pattern.search(res).groups()[0]) if connected else None
            return source, target, net_lag
        except Exception as exception:
            logging.exception(exception)
            return source, target, None

    if not ip_info:
        return {}

    futures = list()
    thread_result = defaultdict(list)
    with ThreadPoolExecutor(max_workers=min(MAX_THREADS, len(ip_info) * N_THREADS)) as pool:
        for (source_ip, target_ip) in ip_info:
            futures.extend([pool.submit(get_ping, source_ip, target_ip) for _ in range(N_THREADS)])

        try:
            for future in as_completed(futures):
                source_ip, target_ip, lag = future.result()
                thread_result[(source_ip, target_ip)].append(lag)
        except concurrent.futures.TimeoutError:
            logging.warning("The ping task was not finished, maybe there are too many nodes.")

    ping_state = defaultdict(list)
    for (source_ip, target_ip), to_primary_dn in ip_info.items():
        if (source_ip, target_ip) not in thread_result:
            continue

        length = len(thread_result[(source_ip, target_ip)])
        lags = [lag for lag in thread_result[(source_ip, target_ip)] if lag is not None]

        state = str(int(len(lags) > 0))
        packet_rate = str(round(len(lags) / length, 5))
        lag = str(round(sum(lags) / length, 5))

        ping_state["source"].append(source_ip)
        ping_state["target"].append(target_ip)
        ping_state["to_primary_dn"].append(to_primary_dn)
        ping_state["state"].append(state)
        ping_state["packet_rate"].append(packet_rate)
        ping_state["lag"].append(lag if lag else None)

    return ping_state


def get_ping_info(name, ping_state):
    """
    To get the very column from ping_state
    - param name: the column name.
    - param ping_state: the ping_state dict.
    """
    if not ping_state:
        return []

    return ping_state.get(name, [])


def get_dummy_value():
    """
    To get a dummy `1`. Some metrics' value didn't contain
    any useful information.
    """
    return ["1"]


def parse_lsblk():
    """
    To parse the output from command `lsblk`
    """
    lsblk = perform_shell_command(cmd="lsblk --output name,kname,type,mountpoint",
                                  stdin="", timeout=1)[1]
    lines = list()
    for line in lsblk.split('\n')[1:]:
        line_contents = line.split()
        if len(line_contents) < 3:
            continue

        if not line_contents[0].startswith(("├", "└", "│", "|", "`", " ")):
            lines.append((line_contents[0], set()))

        if line_contents[-1].startswith("/"):
            lines[-1][1].add((line_contents[-3], line_contents[-1]))
        else:
            lines[-1][1].add((line_contents[-2], ""))

    blk_info = dict()
    for line in lines:
        blk_info[line[0]] = list(line[1])

    return blk_info


def parse_df():
    """
    To parse the output from command `df`
    """
    df = perform_shell_command(cmd="df", stdin="", timeout=5)[1]
    df_info = defaultdict(set)
    for line in df.split('\n')[1:]:
        line_contents = line.split()
        if not line_contents:
            continue

        if line_contents[-1].startswith("/"):
            usage = str(int(line_contents[-4]) / (int(line_contents[-4]) + int(line_contents[-3])) * 100)
            df_info[line_contents[0]].add((usage, line_contents[-1]))
        else:
            df_info[line_contents[0]].add(("", ""))

    return df_info


def get_local_ips(ip_list):
    """
    To parse the output from command `hostname -I`
    """
    new_ip_list = perform_shell_command(cmd="hostname -I", stdin="", timeout=1)[1].split()
    if not new_ip_list:
        cmd = ("ip a | grep -E 'inet|inet6' | egrep -v '127.0.0.1/|::1/|"
               "scope link' | sed -z 's/\// /g' | awk '{print $2}'")
        new_ip_list = perform_shell_command(cmd=cmd, stdin="", timeout=1)[1].split("\n")

    if not isinstance(ip_list, list):
        return sorted(new_ip_list)

    ip_set = set(ip_list)
    new_ip_set = set(new_ip_list)
    if new_ip_set.issubset(ip_set):  # nic temporarily down
        return sorted(list(ip_set))

    return sorted(list(new_ip_set))


def match_paths(directory, mount):
    if mount == "/":
        return 0.5
    n_matches = 0
    dir_list = directory.split('/')[1:]
    mount_dir_list = mount.split('/')[1:]
    cur = 0
    for idx, mount_directory in enumerate(mount_dir_list):
        if mount_directory == dir_list[cur]:
            cur += 1
            n_matches += 1
        else:
            cur = 0
            n_matches = 0

    return n_matches


def get_mount_device(path, blk_info, df_info):
    """
    To get mount device information with blk_info and df_info and data node's path
    - param path: the type-name of cluster node to be parsed.
    - param blk_info: the return dict from parse_lsblk.
    - param df_info: the return dict from parse_df.
    """

    kname = ""
    dev = ""
    if isinstance(blk_info, dict):
        max_matches = 0
        for device, mount_info in blk_info.items():
            for kernel_name, mount_point in mount_info:
                matches = match_paths(path, mount_point)
                if matches > max_matches:
                    max_matches = matches
                    kname = kernel_name
                    dev = device

    fs = ""
    use = ""
    if isinstance(df_info, dict):
        max_matches = 0
        for device, mount_info in df_info.items():
            for usage, mount_point in mount_info:
                matches = match_paths(path, mount_point)
                if matches > max_matches:
                    max_matches = matches
                    fs = device
                    use = usage

    return [dev, fs, kname, use]


def get_mount_state(cluster_state, process_state, local_ips, blk_info, df_info):
    """
    To get the detailed mount info for data node(device and file-system)
    - param cluster_state: the cluster_state dict.
    - param process_state: the process_state dict.
    - param local_ips: the return list from get_local_ips.
    - param blk_info: the return dict from parse_lsblk.
    - param df_info: the return dict from parse_df.
    """

    mount_state = defaultdict(list)
    if not cluster_state or not process_state:
        return mount_state

    port_list, cwd_list = process_state["port"], process_state["cwd"]
    for role in ["cn", "dn"]:
        state = role + "_state"
        node_state = cluster_state.get(state)
        try:
            real_state = json.loads(node_state[0])
        except json.decoder.JSONDecodeError:
            continue

        for line in real_state:
            ip, port, instance_id = line.get('ip'), line.get('port'), line.get('instance_id')
            if ip not in local_ips or port not in port_list:
                continue

            path = cwd_list[port_list.index(port)]
            device, file_system, kernel_name, usage = get_mount_device(path, blk_info, df_info)
            mount_state["role"].append(role)
            mount_state["ip"].append(ip)
            mount_state["port"].append(port)
            mount_state["path"].append(path)
            mount_state["device"].append(device)
            mount_state["file_system"].append(file_system)
            mount_state["kernel_name"].append(kernel_name)
            mount_state["usage"].append(usage)
            mount_state["instance_id"].append(instance_id)

    return mount_state


def get_mount_info(name, mount_state):
    """
    To get the very column from mount_state
    - param name: the column name.
    - param mount_state: the mount_state dict.
    """
    if not mount_state:
        return []

    return mount_state.get(name, [])


def get_chroot_prefix(cluster_state, process_state, local_ips):
    if not cluster_state or not process_state:
        return ""

    port_list, cwd_list = process_state["port"], process_state["cwd"]

    dn_state = cluster_state.get("dn_state")[0]
    try:
        real_state = json.loads(dn_state)
    except json.decoder.JSONDecodeError:
        return ""

    for line in real_state:
        ip, port, short_path = line.get('ip'), line.get('port'), line.get('path')
        if ip not in local_ips:
            continue

        long_path = cwd_list[port_list.index(port)]
        if long_path.endswith(short_path):
            return long_path[:-len(short_path)]

    return ""


def get_xlog_count(xlog_path):
    """
    To count the number of the data node's xlog files.
    - param local_ips: the return list from get_local_ips.
    - param dn_state: the dn state output.
    """

    if not isinstance(xlog_path, str) or not xlog_path:
        return {"count": "0", "oldest_lsn": "-1"}

    try:
        xlog_count = 0
        oldest_xlog = "F" * 16
        for item in os.listdir(xlog_path):
            if os.path.isfile(os.path.join(xlog_path, item)):
                xlog_count += 1

            lsn_str = item[-16:]
            if len(lsn_str) != 16:
                continue

            try:
                int(lsn_str[:8] + lsn_str[-2:], 16)
            except ValueError:
                continue

            oldest_xlog = min(oldest_xlog, lsn_str)

        oldest_lsn = int(oldest_xlog[:8] + oldest_xlog[-2:], 16)
    except FileNotFoundError:
        logging.warning("xlog path: %s didn't exist.", xlog_path)
        return {"count": "0", "oldest_lsn": "-1"}

    return {"count": str(xlog_count), "oldest_lsn": str(oldest_lsn)}


def parse_node_state(node_state):
    if node_state is None:
        return []

    try:
        node_state_obj = json.loads(node_state[0])
    except json.decoder.JSONDecodeError:
        return []

    if not isinstance(node_state_obj, list):
        return []

    return node_state_obj


def get_xlog_state(local_ips, dn_state, cn_state, chroot_prefix, blk_info, df_info):
    """
    To get the data node instances
    - param local_ips: the return list from get_local_ips.
    - param dn_state: the data node state output.
    - param cn_state: the coordinator state output.
    - param chroot_prefix: amend the path with chroot_prefix
    - param blk_info: the return dict from parse_lsblk.
    - param df_info: the return dict from parse_df.
    """

    xlog_cmd = "cm_ctl view -N | sed -n '/datanodeInstanceID :{}/,/datanodeXlogPath/p' | tail -n 1"
    xlog_state = defaultdict(list)
    cluster = {"cn": parse_node_state(cn_state), "dn": parse_node_state(dn_state)}
    for role, nodes in cluster.items():
        for node in nodes:
            ip, port = node.get('ip'), node.get('port')
            instance_id, path = node.get('instance_id'), node.get('path')
            if ip not in local_ips:
                continue

            if role == "dn":
                try:
                    cm_ctl_view = perform_shell_command(
                        cmd=xlog_cmd.format(instance_id),
                        stdin="",
                        timeout=1
                    )[1]
                    xlog_path = cm_ctl_view.split('\n')[0].split(":")[1].strip()
                except IndexError:
                    xlog_path = None

                if not xlog_path:
                    xlog_path = os.path.join(path, 'pg_xlog')
            else:
                xlog_path = os.path.join(path, 'pg_xlog')

            xlog_path = chroot_prefix + xlog_path if chroot_prefix else xlog_path
            _, fs, _, _ = get_mount_device(xlog_path, blk_info, df_info)
            if not fs:
                continue

            xlog_count = get_xlog_count(xlog_path)

            for ip in local_ips:
                from_instance = f"{prepare_ip(ip)}:{port}"
                xlog_state["role"].append(role)
                xlog_state["path"].append(xlog_path)
                xlog_state["from_instance"].append(from_instance)
                xlog_state["device"].append(fs)
                xlog_state["count"].append(xlog_count["count"])
                xlog_state["oldest_lsn"].append(xlog_count["oldest_lsn"])
                xlog_state["instance_id"].append(instance_id)

    return xlog_state


def get_xlog_info(name, xlog_state):
    """
    To get the very column from xlog_count
    - param name: the column name.
    - param xlog_state: the xlog_state dict.
    """
    if not xlog_state:
        return []

    return xlog_state.get(name, [])


def get_ip(local_ips):
    """
    To get the multiple ip addresses of the node
    - param local_ips: the return list from get_local_ips.
    """
    return [json.dumps(local_ips)]


PROCESS_METHODS = {
    "get_shell_output": get_shell_output,
    "get_shell_output_lines": get_shell_output_lines,
    "get_process_state": get_process_state,
    "get_ps_info": get_ps_info,
    "parse_state_detail": parse_state_detail,
    "parse_dn_state": parse_dn_state,
    "parse_ip_info": parse_ip_info,
    "get_ping_state": get_ping_state,
    "get_ping_info": get_ping_info,
    "get_dummy_value": get_dummy_value,
    "parse_lsblk": parse_lsblk,
    "parse_df": parse_df,
    "get_local_ips": get_local_ips,
    "get_mount_state": get_mount_state,
    "get_mount_info": get_mount_info,
    "get_chroot_prefix": get_chroot_prefix,
    "get_xlog_state": get_xlog_state,
    "get_xlog_info": get_xlog_info,
    "get_ip": get_ip,
}
