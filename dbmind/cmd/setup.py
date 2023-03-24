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
import getpass
import os
import shutil

from dbmind import constants, global_vars
from dbmind.cmd.configs.config_utils import (
    load_sys_configs,
    check_config_validity,
    config_is_null_value,
    config_set_value_encrypted_flag,
    config_is_encrypted_value,
    config_standardize_null_value,
    get_config_security_keys,
    set_config_encryption_iv,
    create_dynamic_configs)
from dbmind.cmd.configs.configurators import UpdateConfig, DynamicConfig, GenerationConfig
from dbmind.common import utils, security
from dbmind.common.exceptions import SetupError, SQLExecutionError, DuplicateTableError
from dbmind.metadatabase import (
    create_metadatabase_schema,
    destroy_metadatabase
)


def initialize_and_check_config(confpath, interactive=False, quiet=False):
    """Returns true while configration procedure is successful. Otherwise,
    returns default value None."""
    if not os.path.exists(confpath):
        raise SetupError('Not found the directory %s.' % confpath)
    confpath = os.path.realpath(confpath)  # in case of dir changed.
    os.chdir(confpath)
    dbmind_conf_path = os.path.join(confpath, constants.CONFILE_NAME)
    dynamic_config_path = os.path.join(confpath, constants.DYNAMIC_CONFIG)

    if not os.path.exists(dynamic_config_path):
        # If dynamic config file does not exist, create a new one.
        s1, s2 = create_dynamic_configs()
    else:
        # If exists, need not create a new dynamic config file
        # and directly load hash key s1 and s2 from it.
        s1, s2 = get_config_security_keys()
        if not (s1 and s2):
            # If s1 or s2 is invalid, it indicates that an broken event may occurred while generating
            # the dynamic config file. Hence, the whole process of generation is unreliable and we have to
            # generate a new dynamic config file.
            os.unlink(dynamic_config_path)
            s1, s2 = create_dynamic_configs()

    # Check some configurations and encrypt passwords.
    with UpdateConfig(dbmind_conf_path) as config:
        if not interactive:
            for section, section_comment in config.sections():
                for option, value, inline_comment in config.items(section):
                    valid, invalid_reason = check_config_validity(
                        section, option, value
                    )
                    if not valid:
                        raise SetupError(
                            "Wrong %s-%s in the file dbmind.conf due to '%s'. Please revise it." % (
                                section, option, invalid_reason
                            )
                        )

        utils.cli.write_to_terminal('Starting to encrypt the plain-text passwords in the config file...',
                                    color='green')
        for section, section_comment in config.sections():
            for option, value, inline_comment in config.items(section):
                if 'password' in option and not config_is_null_value(value):
                    # Skip when the password has encrypted.
                    if config_is_encrypted_value(value):
                        continue
                    # Every time a new password is generated, update the IV.
                    iv = security.generate_an_iv()
                    set_config_encryption_iv(iv, section, option)
                    cipher_text = security.encrypt(s1, s2, iv, value)
                    decorated_cipher_text = config_set_value_encrypted_flag(cipher_text)
                    config.set(section, option, decorated_cipher_text, inline_comment)

    # config and initialize meta-data database.
    utils.cli.write_to_terminal('Starting to initialize and check the essential variables...',
                                color='green')
    global_vars.dynamic_configs = DynamicConfig()
    global_vars.configs = load_sys_configs(
        constants.CONFILE_NAME
    )
    utils.cli.write_to_terminal('Starting to connect to meta-database and create tables...',
                                color='green')
    try:
        create_metadatabase_schema(check_first=False)
        utils.cli.write_to_terminal('The setup process finished successfully.', color='green')
        return True  # return true in this branch
    except (DuplicateTableError, SQLExecutionError) as e:
        if 'already exist' not in str(e):
            utils.cli.write_to_terminal('Failed to link metadatabase due to unknown error (%s), '
                                        'please check the database and its configuration.' % e,
                                        color='red')
            return

        def override():
            utils.cli.write_to_terminal('Starting to drop existent tables in meta-database...',
                                        color='green')
            destroy_metadatabase()
            utils.cli.write_to_terminal('Starting to create tables for meta-database...', color='green')
            create_metadatabase_schema(check_first=True)

        if not quiet:
            utils.cli.write_to_terminal('The given database has duplicate tables. '
                                        'If you want to reinitialize the database, press [R]. '
                                        'If you want to keep the existent tables, press [K].', color='red')
            input_char = ''
            while input_char not in ('R', 'K'):
                input_char = input('Press [R] to reinitialize; Press [K] to keep and ignore:').upper()
            if input_char == 'R':
                override()
            if input_char == 'K':
                utils.cli.write_to_terminal('Ignoring...', color='green')
        else:
            override()

        # If successful, refresh the version number.
        shutil.copy(
            src=os.path.join(constants.MISC_PATH, constants.VERFILE_NAME),
            dst=os.path.join(confpath, constants.VERFILE_NAME)
        )
        utils.cli.write_to_terminal('The setup process finished successfully.', color='green')
        return True  # return true in this branch
    except Exception as e:
        utils.cli.write_to_terminal('Failed to connect to metadatabase due to unknown error (%s), '
                                    'please check the database and its configuration.' % e, color='red')


def setup_directory_interactive(confpath):
    # Determine whether the directory is empty.
    if os.path.exists(confpath) and len(os.listdir(confpath)) > 0:
        raise SetupError("Given setup directory '%s' already exists." % confpath)

    # Make the confpath directory and copy all files
    # (basically all files are config files) from MISC directory.
    shutil.copytree(
        src=constants.MISC_PATH,
        dst=confpath
    )
    utils.base.chmod_r(confpath, 0o700, 0o600)

    utils.cli.write_to_terminal('Starting to configure...', color='green')
    # Generate an initial configuration file.
    with GenerationConfig(
        filepath_src=os.path.join(
            constants.MISC_PATH, constants.CONFILE_NAME),
        filepath_dst=os.path.join(
            confpath, constants.CONFILE_NAME)
    ) as config:
        try:
            # Modify configuration items by user's typing.
            for section in config.sections():
                section_comment = config.get('COMMENT', section, fallback='')
                utils.cli.write_to_terminal('[%s]' % section, color='white')
                utils.cli.write_to_terminal(section_comment, color='yellow')
                # Get each configuration item.
                for option, values in config.items(section):
                    try:
                        default_value, inline_comment = map(str.strip, values.rsplit('#', 1))
                    except ValueError:
                        default_value, inline_comment = values.strip(), ''
                    default_value = config_standardize_null_value(
                        default_value
                    )
                    # hidden password
                    input_value = ''
                    if 'password' in option:
                        input_func = getpass.getpass
                    else:
                        input_func = input

                    while input_value.strip() == '':
                        # Ask for options.
                        input_value = input_func(
                            '%s\n'
                            '  [default: %s]\n'
                            '  (%s)\n' %
                            (option, default_value, inline_comment))
                        # If user does not set the option, set default target.
                        if input_value.strip() == '':
                            input_value = default_value

                        valid, invalid_reason = check_config_validity(
                            section, option, input_value
                        )
                        if not valid:
                            utils.cli.write_to_terminal(
                                "Please retype due to '%s'." % invalid_reason,
                                level='error',
                                color='red'
                            )
                            input_value = ''
                    config.set(section, option, '%s  # %s' % (
                        input_value, inline_comment))
        except (KeyboardInterrupt, EOFError, ValueError):
            utils.cli.write_to_terminal('Removing generated files due '
                                        'to exception.')
            shutil.rmtree(
                path=confpath
            )
            return

    initialize_and_check_config(confpath, interactive=True)


def setup_directory(confpath):
    # Determine whether the directory is empty.
    if os.path.exists(confpath) and len(os.listdir(confpath)) > 0:
        raise SetupError("Given setup directory '%s' already exists." % confpath)

    utils.cli.write_to_terminal(
        "You are not in the interactive mode so you must modify "
        "configurations manually.\n"
        "The file you need to modify is '%s'.\n"
        "After configuring, you should continue to set up and initialize "
        "the directory with --initialize option, "
        "e.g.,\n "
        "'... service setup -c %s --initialize'"
        % (os.path.join(confpath, constants.CONFILE_NAME), confpath),
        color='yellow')

    # Make the confpath directory and copy all files
    # (basically all files are config files) from MISC directory.
    shutil.copytree(
        src=constants.MISC_PATH,
        dst=confpath
    )
    utils.base.chmod_r(confpath, 0o700, 0o600)
    with GenerationConfig(
        filepath_src=os.path.join(confpath, constants.CONFILE_NAME),
        filepath_dst=os.path.join(confpath, constants.CONFILE_NAME),
    ):
        pass

    utils.cli.write_to_terminal("Configuration directory '%s' has "
                                "been created successfully." % confpath,
                                color='green')
