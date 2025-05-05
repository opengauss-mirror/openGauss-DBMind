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
"""Command Line Interface"""

import argparse
import functools
import os
import psycopg2
import subprocess

import dbmind
from dbmind import components as components_module
from dbmind import constants
from dbmind import global_vars
from dbmind.common.exceptions import SetupError, ConfigSettingError, InitializationError
from dbmind.common.utils.checking import path_type
from dbmind.common.utils.cli import set_proc_title, write_to_terminal
from dbmind.constants import __description__, __version__
from . import edbmind
from . import setup
from .configs import config_utils


psycopg2.connect = functools.partial(psycopg2.connect, sslmode='prefer')


def build_parser():
    actions = ['setup', 'start', 'stop', 'restart', 'reload']
    # Create the top-level parser to parse the common action.
    parser = argparse.ArgumentParser(
        description=__description__
    )
    parser.add_argument('-v', '--version', action='version', version=__version__)

    # Add sub-commands:
    subparsers = parser.add_subparsers(title='available subcommands',
                                       help="type '<subcommand> -h' for help on a specific subcommand",
                                       dest='subcommand')
    # Create the parser for the "service" command.
    parser_service = subparsers.add_parser('service', help='send a command to DBMind to change the status of '
                                                           'the service')

    parser_service.add_argument('action', choices=actions, help='perform an action for service')
    # This type should not be path_type because path_type will validate if the path exists.
    parser_service.add_argument('-c', '--conf', type=os.path.realpath, metavar='DIRECTORY', required=True,
                                help='set the directory of configuration files')
    parser_service.add_argument('--only-run', choices=constants.TASK_NAMES,
                                help='explicitly set a certain task running in the backend')
    parser_service.add_argument('--dry-run', action='store_true',
                                help='run the backend task(s) once. '
                                     'the task to run can be specified by the --only-run argument')
    parser_service.add_argument('-f', '--force', action='store_true', default=False,
                                help='force to stop the process and cancel all in-progress tasks')
    config_mode_group = parser_service.add_mutually_exclusive_group()
    config_mode_group.add_argument('--interactive', action='store_true',
                                   help='configure and initialize with interactive mode')
    config_mode_group.add_argument('--initialize', action='store_true',
                                   help='initialize and check configurations after configuring.')

    # Create the parser for the "set" command.
    parser_set = subparsers.add_parser('set', help='set a parameter')
    parser_set.add_argument('section', help='which section (case sensitive) to set')
    parser_set.add_argument('option', help='which option to set')
    parser_set.add_argument('target', help='the parameter target to set')
    parser_set.add_argument('-c', '--conf', type=path_type, metavar='DIRECTORY', required=True,
                            help='set the directory of configuration files')

    # Create the parser for the "component" command.
    # This component includes Prometheus-exporter and other components that can be
    # run independently through the command line.
    # Components can be easily extended, similar to a plug-in.
    # The component need to be called can import DBMind packages directly.
    components = components_module.list_components()
    parser_component = subparsers.add_parser('component',
                                             help='pass command line arguments to each sub-component.')
    parser_component.add_argument('name', metavar='COMPONENT_NAME', choices=components,
                                  help='choice a component to start. ' + str(components))
    parser_component.add_argument('arguments', metavar='ARGS', nargs=argparse.REMAINDER,
                                  help='arguments for the component to start')
    return parser


def start(args):
    # Determine which task runs in the backend.
    if args.only_run is not None:
        global_vars.default_timed_task.append(args.only_run)
    global_vars.is_dry_run_mode = args.dry_run
    edbmind.DBMindMain(args.conf).start()


class DBMindRun:
    """Helper class to use as main for DBMind:

    DBMindRun(*sys.argv[1:])
    """

    def __init__(self, argv):
        os.umask(0o0077)
        exitcode = 0

        parser = build_parser()
        args = parser.parse_args(argv)

        def check_process_exists():
            pid_file = os.path.join(args.conf, constants.PIDFILE_NAME)
            if os.path.exists(pid_file):
                with open(pid_file) as f:
                    pid = f.readline().strip().strip('\n')
                if not pid.isdigit():
                    # If pid file is modified illegally, we consider process existing and refuse to initialize.
                    return True
                else:
                    process_info = subprocess.Popen(
                        ["ps", "-w", "w", "-p", f"{pid}"], stdout=subprocess.PIPE, shell=False
                    ).communicate(timeout=15)
                    if "DBMind [Master Process]" in process_info[0].decode().split("\n")[1]:
                        return True
                    else:
                        return False
            else:
                return False

        try:
            if args.subcommand == 'service':
                if args.action == 'setup':
                    if args.interactive:
                        setup.setup_directory_interactive(args.conf)
                    elif args.initialize:
                        try:
                            process_status = check_process_exists()
                        except Exception:
                            raise InitializationError("Failed to check process status, initializing not executed,"
                                                      " please check DBMind process and dbmind.pid under config path.")
                        if process_status:
                            raise InitializationError("Setup initialization is not allowed while DBMind running.")
                        else:
                            setup.initialize_and_check_config(args.conf, interactive=False)
                    else:
                        setup.setup_directory(args.conf)
                elif args.action == 'start':
                    start(args)
                elif args.action == 'stop':
                    if args.force:
                        edbmind.DBMindMain(args.conf).stop(level='mid')
                    else:
                        edbmind.DBMindMain(args.conf).stop()
                elif args.action == 'restart':
                    if args.force:
                        edbmind.DBMindMain(args.conf).stop(level='mid')
                    else:
                        edbmind.DBMindMain(args.conf).stop()
                    start(args)
                elif args.action == 'reload':
                    edbmind.DBMindMain(args.conf).reload()
                else:
                    parser.print_usage()
            elif args.subcommand == 'show':
                pass
            elif args.subcommand == 'set':
                set_proc_title('setting')
                config_utils.set_config_parameter(args.conf, args.section, args.option, args.target)
            elif args.subcommand == 'component':
                components_module.call_component(args.name, args.arguments)
            else:
                parser.print_usage()
        except (SetupError, ConfigSettingError) as e:
            write_to_terminal(e, color='red')
            exitcode = 2
        except InitializationError as e:
            write_to_terminal(e, color='red')
            exitcode = 3

        # return exitcode to identify status
        exit(exitcode)
