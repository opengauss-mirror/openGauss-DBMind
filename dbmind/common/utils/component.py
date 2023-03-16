import logging

from prettytable import PrettyTable

from dbmind import global_vars
from dbmind.cmd.edbmind import init_rpc_with_config, init_tsdb_with_config
from dbmind.common.utils import raise_fatal_and_exit
from dbmind.common.utils.cli import keep_inputting_until_correct


def initialize_rpc_service():
    try:
        proxy = init_rpc_with_config()
        proxy.finalize_agents()
        choose_an_rpc()
        result = global_vars.agent_proxy.call('query_in_database',
                                              'select 1',
                                              'postgres',
                                              return_tuples=True)
        return result[0][0] == 1
    except Exception as e:
        logging.exception(e)
        return False


def initialize_tsdb_param():
    try:
        tsdb = init_tsdb_with_config()
        return tsdb.check_connection()
    except Exception as e:
        logging.warning(e)
        return False


def choose_an_rpc():
    all_agents = global_vars.agent_proxy.get_all_agents()
    if len(all_agents) == 1:
        return

    prompt = PrettyTable()
    prompt.field_names = ('NUMBER', 'Agent', 'Cluster Nodes')
    options = []
    agent_addr = []
    for no, agent in enumerate(all_agents):
        prompt.add_row((str(no), str(agent), '\n'.join(all_agents[agent])))
        options.append(str(no))
        agent_addr.append(agent)
    prompt_msg = (str(prompt) + '\nPlease type a NUMBER to choose an RPC agent:')
    try:
        no = keep_inputting_until_correct(prompt_msg, options)
    except KeyboardInterrupt:
        raise_fatal_and_exit('\nNot selected an RPC agent, exiting...')
    else:
        if not global_vars.agent_proxy.switch_context(agent_addr[int(no)]):
            raise AssertionError()
