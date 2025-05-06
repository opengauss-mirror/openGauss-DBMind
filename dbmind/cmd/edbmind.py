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
"""DBMind common functionality interface"""
import functools
import logging
import os
import psycopg2
import signal
import sys
import threading
import time
import traceback
from logging.handlers import RotatingFileHandler

from dbmind import app
from dbmind import constants
from dbmind import controllers
from dbmind import global_vars
from dbmind.cmd.configs.config_utils import (
    load_sys_configs
)
from dbmind.cmd.configs.configurators import DynamicConfig
from dbmind.common import platform
from dbmind.common import utils
from dbmind.common.daemon import (Daemon, read_dbmind_pid_file, clean_dbmind_process, get_children_pid_file,
                                  check_parent_child_process)
from dbmind.common.dispatcher import TimedTaskManager
from dbmind.common.dispatcher import get_worker_instance
from dbmind.common.exceptions import (SetupError, CertCheckException, ConfigSettingError, WeakPasswordException,
                                      WeakPrivateKeyException)
from dbmind.common.http import HttpService
from dbmind.common.tsdb import TsdbClientFactory
from dbmind.common.utils.exporter import is_port_used
from dbmind.common.parser.others import extract_ip_groups
from dbmind.common.security import check_password_strength, is_private_key_encrypted, CertCheckerHandler
# Support input() function in using backspace.
from dbmind.metadatabase.result_db_session import session_clz, update_session_clz_from_configs
from dbmind.service.agent_factory import RegularAgent, DistributedAgent

from .thread_context import ContextProxy

try:
    import readline
except ImportError:
    pass

_http_service = HttpService()
dbmind_master_should_exit = False


def _get_ip_map():
    ip_map = dict()
    ip_map_content = global_vars.configs.get('IP_MAP', 'ip_map')
    if not ip_map_content:
        return ip_map

    for ip_group in extract_ip_groups(ip_map_content):
        ip_pairs_dict = dict([tuple(ip_pair[::-1]) for ip_pair in ip_group])
        for connection_ip in ip_pairs_dict.values():
            ip_map[connection_ip] = ip_pairs_dict.copy()

    return ip_map


def _get_is_distribute_mode():
    mode_status = global_vars.configs.get('AGENT', 'distribute_mode')
    distribute_mode = True if mode_status.strip() == 'true' else False
    return distribute_mode


def _check_confpath(confpath):
    confile_path = os.path.join(confpath, constants.CONFILE_NAME)
    if os.path.exists(confile_path):
        return True
    return False


def _process_clean(force=False):
    # Wait for workers starting.
    while global_vars.worker is None:
        time.sleep(.1)
    global_vars.worker.terminate(cancel_futures=force)
    TimedTaskManager.stop()
    _http_service.shutdown()


def signal_handler(signum, frame):
    global dbmind_master_should_exit

    if signum == signal.SIGINT or signum == signal.SIGHUP:
        utils.cli.write_to_terminal('Reloading parameters.', color='green')
        logging.info('Reloading parameters.')
        configs = global_vars.configs
        try:
            global_vars.configs = load_sys_configs(constants.CONFILE_NAME)
        except (ConfigSettingError, SetupError) as e:
            error_info = f'Failed to reload parameters: {e}'
            utils.cli.write_to_terminal(error_info, color='red')
            logging.error(error_info)
            global_vars.configs = configs
            return
        global_vars.ip_map = _get_ip_map()
        global_vars.is_distribute_mode = _get_is_distribute_mode()
        if not global_vars.is_distribute_mode:
            TimedTaskManager.specified_timed_task = get_timed_tasks()
            if constants.SLOW_SQL_DIAGNOSIS in TimedTaskManager.specified_timed_task:
                TimedTaskManager.specified_timed_task.remove(constants.SLOW_SQL_DIAGNOSIS)
                TimedTaskManager.specified_timed_task.append(constants.SLOW_QUERY_DIAGNOSIS)
            init_anomaly_detection_pool()
            # refresh timed-tasks
            TimedTaskManager.flush()
    elif signum == signal.SIGUSR2:
        # used for debugging
        with open('traceback.stack', 'w+') as f:
            f.write('======== Thread Information ========\n')
            for th in threading.enumerate():
                f.write(f'{str(th)} NativeID: {th.native_id} Ident: {th.ident}\n')
                current_frames = getattr(sys, '_current_frames')()
                traceback.print_stack(current_frames[th.ident], file=f)
                f.write('\n')
    elif signum == signal.SIGTERM:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        logging.info('DBMind received exit signal.')
        logging.info('Cleaning resources...')
        utils.cli.write_to_terminal('Cleaning resources...')
        dbmind_master_should_exit = True
        _process_clean()
    elif signum == signal.SIGQUIT:
        # Force to exit and cancel future tasks.
        signal.signal(signal.SIGQUIT, signal.SIG_IGN)
        logging.info('DBMind received the signal: exit immediately.')
        logging.info('Cleaning resources...')
        utils.cli.write_to_terminal('Cleaning resources...')
        dbmind_master_should_exit = True
        _process_clean(force=True)


def init_rpc_with_config(tsdb=None):
    distribute_mode = True if global_vars.configs.get('AGENT', 'distribute_mode').strip() == 'true' else False
    if distribute_mode:
        global_vars.is_distribute_mode = True
        global_vars.agent_proxy_setter = ContextProxy("agent_proxy_setter")
        global_vars.agent_proxy = ContextProxy("agent_proxy")
        return None

    global_vars.is_distribute_mode = False
    global_vars.agent_proxy_setter = RegularAgent()

    master_url = utils.split(global_vars.configs.get('AGENT', 'master_url'))
    ssl_certfile = utils.split(global_vars.configs.get('AGENT', 'ssl_certfile'))
    ssl_keyfile = utils.split(global_vars.configs.get('AGENT', 'ssl_keyfile'))
    ssl_keyfile_password = utils.split(global_vars.configs.get('AGENT', 'ssl_keyfile_password'))
    ssl_ca_file = utils.split(global_vars.configs.get('AGENT', 'ssl_ca_file'))
    agent_username = utils.split(global_vars.configs.get('AGENT', 'username'))
    agent_pwd = utils.split(global_vars.configs.get('AGENT', 'password'))

    if ssl_certfile:
        for keyfile in ssl_keyfile:
            if not is_private_key_encrypted(keyfile):
                raise WeakPrivateKeyException('Unencrypted AGENT SSL private key is forbidden.')
        if len(ssl_keyfile) != len(ssl_keyfile_password):
            raise WeakPrivateKeyException('Unencrypted AGENT SSL private key is forbidden.')
        for keyfile_pwd in ssl_keyfile_password:
            if not keyfile_pwd:
                raise WeakPrivateKeyException('Unencrypted AGENT SSL private key is forbidden.')
            if not check_password_strength(keyfile_pwd, username=None, is_ssl_keyfile_password=True):
                raise WeakPasswordException('AGENT SSL private key password does not meet the strength requirements.')
        if len(ssl_ca_file) != len(ssl_certfile):
            raise CertCheckException('Invalid AGENT SSL CA file or CERT file.')
        for i in range(len(ssl_ca_file)):
            if not CertCheckerHandler.is_valid_cert(ca_name=ssl_ca_file[i], crt_name=ssl_certfile[i]):
                raise CertCheckException('Invalid AGENT SSL CA file or CERT file.')

    # -- check for agent configurations below --
    def is_the_same_length(*args):
        idx = 1
        while idx < len(args):
            if len(args[idx]) != len(args[idx - 1]):
                return False
            idx += 1
        return True

    error_msg = ("The agent ssl configuration is incorrect, "
                 "keep the number of setting elements of "
                 "these configurations the same.")

    if (ssl_keyfile_password and (not is_the_same_length(
            ssl_certfile, ssl_keyfile, ssl_keyfile_password,
            ssl_ca_file
    ))) or (not is_the_same_length(ssl_certfile, ssl_keyfile, ssl_ca_file)):
        utils.raise_fatal_and_exit(error_msg, use_logging=False)

    if not is_the_same_length(
            agent_username, agent_pwd
    ):
        utils.raise_fatal_and_exit(
            error_msg, use_logging=False
        )
    # If master url doesn't set, we don't need to do the
    # subsequent checks.
    if len(master_url) > 0 and not is_the_same_length(
            master_url, ssl_certfile
    ) and len(ssl_certfile) > 1:
        utils.raise_fatal_and_exit(
            error_msg, use_logging=False
        )
    if len(master_url) > 0 and not is_the_same_length(
            master_url, agent_username
    ) and len(ssl_certfile) > 1:
        utils.raise_fatal_and_exit(
            error_msg, use_logging=False
        )

    # -- finish checking --
    # If user doesn't set any urls, we can
    # try to scan and discover the urls by TSDB metrics.
    # But auto-discover has a restriction, which is
    # user should set a username. Otherwise, we can't
    # connect to the agent with any credentials.
    # Meanwhile, the username should be one element.
    if tsdb and len(master_url) == 0:
        try:
            if len(agent_username) != 1 or len(ssl_certfile) > 1:
                utils.raise_fatal_and_exit(
                    "You haven't set master_url, which means "
                    "you employ autodiscover mode. You only can "
                    "set ONE same username/password/SSL for all cluster.",
                    use_logging=False
                )

            global_vars.agent_proxy_setter.set_agent_info(
                agent_mode='rpc',
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                ssl_keyfile_password=ssl_keyfile_password,
                ssl_ca_file=ssl_ca_file,
                agent_username=agent_username,
                agent_pwd=agent_pwd,
                auto_discover_mode=True,
                tsdb=tsdb
            )
        except Exception as e:
            logging.warning('Cannot extract agent url from TSDB.', exc_info=e)

    else:
        global_vars.agent_proxy_setter.set_agent_info(
            agent_mode='rpc',
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_keyfile_password=ssl_keyfile_password,
            ssl_ca_file=ssl_ca_file,
            agent_username=agent_username,
            agent_pwd=agent_pwd,
            auto_discover_mode=False,
            master_url=master_url
        )

    global_vars.agent_proxy = global_vars.agent_proxy_setter.get_agent()
    logging.info('Agent initialized as RPC mode.')
    return global_vars.agent_proxy


def init_logger_with_config():
    log_directory = global_vars.configs.get('LOG', 'log_directory', fallback='logs')
    log_directory = os.path.realpath(log_directory)
    os.makedirs(log_directory, exist_ok=True)
    max_bytes = global_vars.configs.getint('LOG', 'maxbytes')
    backup_count = global_vars.configs.getint('LOG', 'backupcount')
    logging_handler = utils.MultiProcessingRFHandler(filename=os.path.join(log_directory, constants.LOGFILE_NAME),
                                                     maxBytes=max_bytes,
                                                     backupCount=backup_count)
    logging_handler.setFormatter(
        logging.Formatter("[%(asctime)s %(levelname)s][%(process)d-%(thread)d][%(name)s]: %(message)s")
    )
    logger = logging.getLogger()
    logger.name = 'DBMind'
    logger.addHandler(logging_handler)
    logger.setLevel(global_vars.configs.get('LOG', 'level').upper())
    return logging_handler


def init_warning_logger():
    """This logger contains the py warning information."""
    log_directory = global_vars.configs.get('LOG', 'log_directory', fallback='logs')
    log_directory = os.path.realpath(log_directory)
    log_file_path = os.path.join(log_directory, "warnings.log")
    os.makedirs(log_directory, exist_ok=True)
    logging_handler = RotatingFileHandler(filename=log_file_path)
    logging_handler.setFormatter(
        logging.Formatter("[%(asctime)s %(levelname)s][%(process)d-%(thread)d][%(name)s]: %(message)s")
    )
    logging.captureWarnings(True)
    warnings_logger = logging.getLogger("py.warnings")
    warnings_logger.addHandler(logging_handler)
    warnings_logger.setLevel(logging.WARNING)
    os.chmod(log_file_path, 0o600)


def init_global_configs(confpath):
    global_vars.confpath = confpath
    global_vars.configs = load_sys_configs(constants.CONFILE_NAME)
    global_vars.dynamic_configs = DynamicConfig()
    global_vars.metric_map.update(
        utils.read_simple_config_file(
            constants.METRIC_MAP_CONFIG
        )
    )
    global_vars.metric_value_range_map.update(
        utils.read_simple_config_file(constants.METRIC_VALUE_RANGE_CONFIG)
    )
    global_vars.must_filter_labels = utils.read_simple_config_file(
        constants.MUST_FILTER_LABEL_CONFIG
    )


def init_tsdb_with_config():
    ssl_cert_file = global_vars.configs.get('TSDB', 'ssl_certfile')
    ssl_key_file = global_vars.configs.get('TSDB', 'ssl_keyfile')
    ssl_key_file_password = global_vars.configs.get('TSDB', 'ssl_keyfile_password')
    ssl_ca_file = global_vars.configs.get('TSDB', 'ssl_ca_file')
    if ssl_cert_file:
        if not is_private_key_encrypted(ssl_key_file) or not ssl_key_file_password:
            raise WeakPrivateKeyException('Unencrypted TSDB SSL private key is forbidden.')
        if not check_password_strength(
                ssl_key_file_password,
                username=None,
                is_ssl_keyfile_password=True
        ):
            raise WeakPasswordException('TSDB SSL private key password does not meet the strength requirements.')
        if not CertCheckerHandler.is_valid_cert(
            ca_name=ssl_ca_file,
            crt_name=ssl_cert_file
        ):
            raise CertCheckException('Invalid TSDB SSL CA file or CERT file.')

    # Set the information for TSDB.
    TsdbClientFactory.set_client_info(
        global_vars.configs.get('TSDB', 'name'),
        global_vars.configs.get('TSDB', 'host'),
        global_vars.configs.get('TSDB', 'port'),
        global_vars.configs.get('TSDB', 'username'),
        global_vars.configs.get('TSDB', 'password'),
        ssl_cert_file,
        ssl_key_file,
        ssl_key_file_password,
        ssl_ca_file,
        global_vars.configs.get('TSDB', 'dbname')
    )
    return TsdbClientFactory.get_tsdb_client()


def init_anomaly_detection_pool():
    from dbmind.app.monitoring import ad_pool_manager
    agents = global_vars.agent_proxy.agent_get_all()
    for primary, node_list in agents.items():
        nodes = node_list.copy()
        if primary not in nodes:
            nodes.append(primary)

        ad_pool_manager.init_specific_detections(primary, nodes)


def record_child_process_pid(child_process_file_path):
    if not platform.LINUX:
        return
    parent_pid = os.getpid()
    child_pids = []
    for pid in os.listdir('/proc'):
        if not pid.isdigit() or pid == parent_pid:
            continue
        try:
            if check_parent_child_process(pid, parent_pid):
                child_pids.append(pid)
        except (FileNotFoundError, PermissionError):
            continue
    # format: pid1,pid2,pid3...
    separator = ','
    try:
        with open(child_process_file_path, 'w+') as fp:
            fp.write(separator.join(child_pids))
    except (FileNotFoundError, PermissionError) as e:
        logging.warning('can not record child process pid, because: %s', str(e))


def get_timed_tasks():
    timed_tasks = utils.split(global_vars.configs.get('TIMED_TASK', 'TASK'))
    if constants.DISCARD_EXPIRED_RESULTS not in timed_tasks:
        # 'DISCARD_EXPIRED_RESULTS' does not support user modification. Therefore,
        # if it is found that the task does not exist in the specified_timed_tasks,
        # it will be forcibly added to global_vars.backend_timed_task.
        timed_tasks.append(constants.DISCARD_EXPIRED_RESULTS)
    return timed_tasks


class DBMindMain(Daemon):
    def __init__(self, confpath):
        if not _check_confpath(confpath):
            raise SetupError("Invalid directory '%s', please set up first." % confpath)

        self.confpath = os.path.realpath(confpath)
        self.worker = None

        pid_file = os.path.join(confpath, constants.PIDFILE_NAME)
        super().__init__(pid_file)

    def run(self):
        os.chdir(self.confpath)
        os.umask(0o0077)

        mismatching_message = (
            "The version of the configuration file directory "
            "does not match the DBMind, "
            "please use the '... setup --initialize' command to reinitialize."
        )
        if not os.path.exists(constants.VERFILE_NAME):
            utils.raise_fatal_and_exit(
                mismatching_message, use_logging=False
            )
        with open(constants.VERFILE_NAME) as fp:
            if fp.readline().strip() != constants.__version__:
                utils.raise_fatal_and_exit(
                    mismatching_message, use_logging=False
                )

        utils.cli.set_proc_title('DBMind [Master Process]')
        # Set global variables.
        init_global_configs(self.confpath)

        # Initialize IP_MAP
        global_vars.ip_map = _get_ip_map()

        # Set logger.
        logging_handler = init_logger_with_config()
        init_warning_logger()

        # Warn user of proxies if user set.
        if utils.get_env('http_proxy') or utils.get_env('https_proxy'):
            logging.warning('You set the proxy environment variable (e.g., http_proxy, https_proxy),'
                            ' which may cause network requests to be sent '
                            'through the proxy server, which may cause some network connectivity issues too.'
                            ' You need to make sure that the action is what you expect.')

        # Register signal handler.
        if not platform.WIN32:
            signal.signal(signal.SIGHUP, signal_handler)
            signal.signal(signal.SIGUSR2, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGQUIT, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        # Initialize TSDB.
        try:
            tsdb = init_tsdb_with_config()
        except WeakPrivateKeyException:
            utils.cli.write_to_terminal('ERROR: Unencrypted TSDB SSL private key is not allowed, '
                                        'use a encrypted private key instead, exiting...', color='red')
            return
        except WeakPasswordException:
            utils.cli.write_to_terminal('ERROR: Weak TSDB SSL private key password is not allowed, '
                                        'increase password strength and try again, exiting...', color='red')
            return
        except CertCheckException:
            utils.cli.write_to_terminal('ERROR: Invalid TSDB SSL CA file or CERT file, '
                                        'check the validity period and encryption algorithm, exiting...', color='red')
            return

        # check insecure protocol for DBMind-service
        use_ssl_raw = global_vars.configs.get('WEB-SERVICE', 'ssl', fallback=None)
        if use_ssl_raw.strip() == 'true':
            use_ssl = True
        elif use_ssl_raw.strip() == 'false':
            use_ssl = False
        else:
            use_ssl = None
        ssl_certfile = global_vars.configs.get('WEB-SERVICE', 'ssl_certfile')
        if use_ssl is True:
            if ssl_certfile.strip() in ('(null)', ''):
                utils.cli.write_to_terminal('FATAL: DBMind service using an insecure protocol, '
                                            'please add SSL config in the WEB-SERVICE section, exiting...', color='red')
                return
        elif use_ssl is False:
            # we will check the actual configuration later,
            # so skip it here.
            pass
        else:
            utils.cli.write_to_terminal('ERROR: Please configure the SSL parameters under WEB-SERVICE to '
                                        'avoid information leakage caused by insecure protocols.', color='red')
            return
        # check sslmode for METADATABASE connection
        ssl_mode = global_vars.configs.get('METADATABASE', 'ssl_mode', fallback='prefer')
        if ssl_mode not in ['disable', 'prefer', 'verify-ca']:
            utils.cli.write_to_terminal(f"Error: ssl_mode only support disable or prefer or verify-ca mode."
                                        f" Please make sure the parameters meet the requirements.")
            return
        psycopg2.connect = functools.partial(psycopg2.connect, sslmode=ssl_mode)
        # Initialize RPC agent.
        try:
            init_rpc_with_config(tsdb)
        except WeakPrivateKeyException:
            utils.cli.write_to_terminal('ERROR: Unencrypted AGENT SSL private key is not allowed, '
                                        'use a encrypted private key instead, exiting...', color='red')
            return
        except WeakPasswordException:
            utils.cli.write_to_terminal('ERROR: Weak AGENT SSL private key password is not allowed, '
                                        'increase password strength and try again, exiting...', color='red')
            return
        except CertCheckException:
            utils.cli.write_to_terminal('ERROR: Invalid AGENT SSL CA file or CERT file, '
                                        'check the validity period and encryption algorithm, exiting...', color='red')
            return

        if global_vars.is_distribute_mode:
            logging.info('DBMind is starting as distribute mode.')
        else:
            logging.info('DBMind is starting as single node mode.')

        if global_vars.is_distribute_mode and not use_ssl:
            utils.cli.write_to_terminal('ERROR: SSL is mandatory in distribute mode.', color='red')
            return

        if not global_vars.is_distribute_mode and global_vars.configs.get('TSDB', 'name') == 'ignore':
            utils.cli.write_to_terminal('ERROR: TSDB can be ignored only in microservice deployment.', color='red')
            return

        update_session_clz_from_configs(is_terminal=False)
        if not session_clz:
            utils.cli.write_to_terminal('ERROR: No available connection to metadatabase, '
                                        'the DBMind progress is exiting.', color='red')
            return

        for p in utils.split(global_vars.configs.get('AGENT', 'password')):
            logging_handler.add_sensitive_word(p)

        tsdb_password = global_vars.configs.get('TSDB', 'password')
        if isinstance(tsdb_password, str) and tsdb_password:
            logging_handler.add_sensitive_word(tsdb_password)

        meta_password = global_vars.configs.get('METADATABASE', 'password')
        if isinstance(meta_password, str) and meta_password:
            logging_handler.add_sensitive_word(meta_password)

        # Create executor pool.
        # Notice: we have to initialize the process pool
        # as early as possible because sub-processes were
        # forked from the main process, meanwhile copying
        # all states. These states may include timed tasks,
        # HTTP response buffers,  and other strange variables.
        # This copying behavior can cause many unexpected
        # and hard-to-debug errors. For example, the
        # sub-process raises 'cannot parse JSON' while getting
        # an HTTP response and parsing it, which is caused
        # the sub-process to copy a partial HTTP buffer.
        child_process_file_path = get_children_pid_file(self.pid_file)
        # clean residual process before create process pool
        clean_dbmind_process(child_process_file_path, only_residual=True)
        local_workers = global_vars.configs.getint('WORKER', 'process_num', fallback=-1)
        global_vars.worker = self.worker = get_worker_instance('local', local_workers)
        if self.pid_file:
            child_process_file_path = get_children_pid_file(self.pid_file)
            record_child_process_pid(child_process_file_path)

        # Start timed tasks.
        if not global_vars.is_distribute_mode:
            if not global_vars.default_timed_task:
                global_vars.default_timed_task.extend(get_timed_tasks())
            app.register_timed_app()
            init_anomaly_detection_pool()
            if global_vars.is_dry_run_mode:
                TimedTaskManager.run_once()
                return

            TimedTaskManager.start()
        # Start to create a web service.
        web_service_host = global_vars.configs.get('WEB-SERVICE', 'host')
        web_service_port = global_vars.configs.getint('WEB-SERVICE', 'port')
        ssl_certfile = global_vars.configs.get('WEB-SERVICE', 'ssl_certfile')
        ssl_keyfile = global_vars.configs.get('WEB-SERVICE', 'ssl_keyfile')
        ssl_keyfile_password = global_vars.configs.get('WEB-SERVICE', 'ssl_keyfile_password')
        ssl_ca_file = global_vars.configs.get('WEB-SERVICE', 'ssl_ca_file')
        # Here we determine the actual configuration
        # content of the user and give different prompts.
        if not use_ssl:
            if ssl_certfile:
                utils.cli.write_to_terminal(
                    'WARNING: Since you configured an SSL certificate, '
                    'although ssl option is not true, '
                    'for security reasons, this certificate option '
                    'is still applicable.', color='yellow'
                )
            else:
                utils.cli.write_to_terminal(
                    'WARNING: DBMind service uses an insecure protocol. '
                    'Suggest adding SSL config in the WEB-SERVICE section.', color='yellow'
                )
        if ssl_certfile:
            if not is_private_key_encrypted(ssl_keyfile) or not ssl_keyfile_password:
                utils.cli.write_to_terminal('ERROR: Unencrypted WEB-SERVICE SSL private key is not allowed, '
                                            'use a encrypted private key instead, exiting...', color='red')
                return
            if not check_password_strength(ssl_keyfile_password, username=None, is_ssl_keyfile_password=True):
                utils.cli.write_to_terminal('ERROR: Weak WEB-SERVICE SSL private key password is not allowed, '
                                            'increase password strength and try again, exiting...', color='red')
                return
            if not CertCheckerHandler.is_valid_cert(ca_name=ssl_ca_file, crt_name=ssl_certfile):
                utils.cli.write_to_terminal('ERROR: Invalid WEB-SERVICE SSL CA file or CERT file, '
                                            'check the validity period and encryption algorithm, exiting...',
                                            color='red')
                return

        if is_port_used(web_service_host, web_service_port):
            utils.cli.write_to_terminal('FATAL: DBMind web service port conflicts, exiting...', color='red')
            return

        # Attach rules for web service.
        for c in controllers.get_dbmind_controller():
            _http_service.register_controller_module(c)
        _http_service.mount_static_files(constants.DBMIND_UI_DIRECTORY)

        _http_service.start_listen(
            web_service_host,
            web_service_port,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            ssl_keyfile_password=ssl_keyfile_password,
            ssl_ca_file=ssl_ca_file
        )

        # Main thread will block here under Python 3.7.
        while not dbmind_master_should_exit:
            time.sleep(1)
        logging.info('DBMind will close.')

    def clean(self):
        pass

    def reload(self):
        pid = read_dbmind_pid_file(self.pid_file)
        if pid > 0:
            if platform.WIN32:
                os.kill(pid, signal.SIGINT)
            else:
                os.kill(pid, signal.SIGHUP)
        else:
            utils.cli.write_to_terminal(
                'Invalid DBMind process.',
                level='error',
                color='red'
            )
            os.remove(self.pid_file)
