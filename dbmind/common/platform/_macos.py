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
import subprocess
import psutil


def macos_get_process_cwd(pid):
    """Get process current working directory on macOS.

    :param pid: Process ID
    :return: process's current working directory
    """
    try:
        process = psutil.Process(pid)
        return process.cwd()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return None


def macos_get_process_cmdline(pid):
    """Get process command line on macOS.
    
    :param pid: Process ID
    :return: process's command line
    """
    try:
        process = psutil.Process(pid)
        cmdline = process.cmdline()
        return ' '.join(cmdline) if cmdline else ''
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return None


def macos_get_process_path(pid):
    """Get process executable path on macOS.
    
    :param pid: Process ID  
    :return: process's executable file path
    """
    try:
        process = psutil.Process(pid)
        return process.exe()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return None


def macos_is_process_running(pid):
    """Check if process is running on macOS.
    
    :param pid: Process ID
    :return: True if process is running, False otherwise
    """
    if pid in (os.getppid(), os.getpid()):
        return True
        
    try:
        process = psutil.Process(pid)
        return process.is_running()
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return False
    except psutil.AccessDenied:
        # If we can't access the process, assume it's running
        return True


def macos_set_proc_title(name: str):
    """Set process title on macOS.
    
    :param name: new process title
    """
    try:
        import setproctitle
        setproctitle.setproctitle(name)
    except ImportError:
        # Fallback: try using os.environ manipulation
        try:
            import sys
            sys.argv[0] = name
        except Exception:
            pass


def macos_check_parent_child_process(pid, parent_pid):
    """Check if pid is child process of parent_pid on macOS.
    
    :param pid: Process ID to check
    :param parent_pid: Parent process ID
    :return: True if pid is child of parent_pid, False otherwise
    """
    try:
        process = psutil.Process(int(pid))
        return process.ppid() == int(parent_pid)
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, ValueError):
        return False


def macos_get_child_processes(parent_pid):
    """Get all child processes of a parent process on macOS.
    
    :param parent_pid: Parent process ID
    :return: List of child process PIDs
    """
    child_pids = []
    try:
        parent_process = psutil.Process(parent_pid)
        children = parent_process.children(recursive=True)
        for child in children:
            try:
                child_pids.append(str(child.pid))
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        pass
    return child_pids


def macos_get_network_connections_cmd(port):
    """Get network connections command for macOS using netstat instead of ss.
    
    :param port: Port number to search for
    :return: Command string for getting network connections
    """
    # Use netstat on macOS instead of ss (which doesn't exist)
    return f"netstat -anv | grep {port}"


def macos_get_ps_cmd(pid):
    """Get ps command for macOS.
    
    :param pid: Process ID
    :return: Command string for getting process info
    """
    # macOS ps command uses different flags
    return f"ps -u -p {pid}"


def macos_get_local_ips_cmd():
    """Get command to retrieve local IP addresses on macOS.
    
    :return: Command string for getting local IPs
    """
    # macOS doesn't support hostname -I, use ifconfig instead
    return "ifconfig | grep -E 'inet [0-9]' | grep -v '127.0.0.1' | awk '{print $2}'"


def macos_get_process_cwd_cmd(pid):
    """Get command to retrieve process working directory on macOS.
    
    :param pid: Process ID
    :return: Command string or None if not supported
    """
    # Use lsof to get working directory
    return f"lsof -p {pid} | grep cwd | awk '{{print $9}}'"


def macos_get_process_fds_cmd(pid):
    """Get command to count process file descriptors on macOS.
    
    :param pid: Process ID
    :return: Command string for counting leaked fds
    """
    # Use lsof to get file descriptors count
    return f"lsof -p {pid} | grep '(deleted)' | wc -l" 