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
"""
unittest case for upgrade
"""
import os
import shutil
import unittest
from unittest.mock import MagicMock
from unittest import mock

from dbmind import constants
from dbmind.components.upgrade.upgrade import Upgrader, get_config_version


dbmind_conf = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dbmind_conf')
dbmind_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
config_backup = dbmind_conf + '_backup'
config_new_backup = dbmind_conf + '_new_backup'


def try_check_config_path(upgrader):
    try:
        upgrader.check_config_path()
    except FileNotFoundError:
        pass


class Upgrade_Tester(unittest.TestCase):

    @staticmethod
    def tearDownClass() -> None:
        if os.path.isdir(config_backup) and not os.path.isdir(dbmind_conf):
            os.rename(config_backup, dbmind_conf)
        if os.path.isdir(config_new_backup):
            shutil.rmtree(config_new_backup)

    def test_check_version(self):
        upgrader = Upgrader(dbmind_path, dbmind_conf)
        old_version = upgrader.old_version
        new_version = upgrader.new_version
        upgrader.old_version = '5.0.0'
        with self.assertRaisesRegex(ValueError, 'Lower DBMind versions should be no less than 5.1.0.', ):
            upgrader.check_version()
        upgrader.new_version = '5.0.0'
        upgrader.old_version = '5.1.0'
        with self.assertRaisesRegex(ValueError, 'Lower DBMind versions should be no less than 5.1.0.', ):
            upgrader.check_version()
        upgrader.old_version = '5.1.0'
        upgrader.new_version = '5.1.0'
        with self.assertRaisesRegex(ValueError, f'Cannot upgrade DBMind to the same version: {upgrader.old_version}.'):
            upgrader.check_version()
        upgrader.old_version = '5.1.0'
        upgrader.new_version = '5.2.0'
        upgrader.check_version()

        upgrader.old_version = old_version
        upgrader.new_version = new_version

    def test_get_sorted_upgraded_sql(self):
        upgrader = Upgrader(dbmind_path, dbmind_conf)
        try_check_config_path(upgrader)
        upgrader.old_version = get_config_version(upgrader.config_path)
        upgrader.backup_config_path()
        upgrader.new_version = upgrader.get_new_version()
        new_version = upgrader.new_version
        upgrader.new_version = '5.2.0'
        upgrade_sql_dir = os.path.join(dbmind_path, 'dbmind', 'components', 'upgrade', 'upgrade_sql', 'upgrade')
        metadatabase_sqlite_files = [os.path.join(upgrade_sql_dir, '5.2.0_metadatabase_sqlite_upgrade.sql')]
        metadatabase_opengauss_files = [os.path.join(upgrade_sql_dir, '5.2.0_metadatabase_opengauss_upgrade.sql')]
        dynamic_sqlite_files = [os.path.join(upgrade_sql_dir, '5.2.0_dynamic_sqlite_upgrade.sql')]
        assert metadatabase_sqlite_files == upgrader.get_sorted_upgraded_sql('sqlite', 'metadatabase')
        assert metadatabase_opengauss_files == upgrader.get_sorted_upgraded_sql('opengauss', 'metadatabase')
        assert dynamic_sqlite_files == upgrader.get_sorted_upgraded_sql('sqlite', 'dynamic')
        upgrader.new_version = new_version

    def test_get_sorted_rollbacked_sql(self):
        upgrader = Upgrader(dbmind_path, dbmind_conf)
        try_check_config_path(upgrader)
        upgrader.old_version = get_config_version(upgrader.config_path)
        upgrader.backup_config_path()
        upgrader.new_version = upgrader.get_new_version()
        new_version = upgrader.new_version
        upgrader.old_version = '5.2.0'
        upgrader.new_version = '5.1.0'
        upgrade_sql_dir = os.path.join(dbmind_path, 'dbmind', 'components', 'upgrade', 'upgrade_sql', 'rollback')
        metadatabase_sqlite_files = [os.path.join(upgrade_sql_dir, '5.2.0_metadatabase_sqlite_rollback.sql'),
                                     os.path.join(upgrade_sql_dir, '5.1.2_metadatabase_sqlite_rollback.sql'),
                                     os.path.join(upgrade_sql_dir, '5.1.1_metadatabase_sqlite_rollback.sql')]
        metadatabase_opengauss_files = [os.path.join(upgrade_sql_dir, '5.2.0_metadatabase_opengauss_rollback.sql'),
                                        os.path.join(upgrade_sql_dir, '5.1.2_metadatabase_opengauss_rollback.sql'),
                                        os.path.join(upgrade_sql_dir, '5.1.1_metadatabase_opengauss_rollback.sql')]
        dynamic_sqlite_files = [os.path.join(upgrade_sql_dir, '5.2.0_dynamic_sqlite_rollback.sql'),
                                os.path.join(upgrade_sql_dir, '5.1.2_dynamic_sqlite_rollback.sql'),
                                os.path.join(upgrade_sql_dir, '5.1.1_dynamic_sqlite_rollback.sql')]
        with mock.patch('glob.glob') as mock_glob:
            mock_glob.return_value = [os.path.join(upgrade_sql_dir, '5.0.1_metadatabase_sqlite_rollback.sql')] + \
                                     metadatabase_sqlite_files
            assert metadatabase_sqlite_files == upgrader.get_sorted_upgraded_sql('sqlite', 'metadatabase')
        with mock.patch('glob.glob') as mock_glob:
            mock_glob.return_value = [os.path.join(upgrade_sql_dir, '5.2.1_metadatabase_opengauss_rollback.sql')] + \
                                     metadatabase_opengauss_files
            assert metadatabase_opengauss_files == upgrader.get_sorted_upgraded_sql('opengauss', 'metadatabase')
        with mock.patch('glob.glob') as mock_glob:
            mock_glob.return_value = [os.path.join(upgrade_sql_dir, '5.2.1.1_dynamic_sqlite_rollback.sql')] + \
                                     dynamic_sqlite_files
            assert dynamic_sqlite_files == upgrader.get_sorted_upgraded_sql('sqlite', 'dynamic')
        upgrader.new_version = new_version

    def test_config_upgrade(self):
        upgrader = Upgrader(dbmind_path, dbmind_conf)
        try_check_config_path(upgrader)
        upgrader.old_version = get_config_version(upgrader.config_path)
        upgrader.backup_config_path()
        upgrader.new_version = upgrader.get_new_version()
        upgrader.new_version = '5.2.0'
        upgrader.config_upgrade()
        assert open(os.path.join(upgrader.config_path_tempdir, constants.VERFILE_NAME)).read() == '5.2.0'

    def test_metadata_upgrade(self):
        upgrader = Upgrader(dbmind_path, dbmind_conf)
        upgrader.new_version = '5.2.0'
        config_path_tempdir = upgrader.config_path_tempdir
        upgrader.config_path_tempdir = os.path.realpath(os.path.join(os.path.dirname(os.path.dirname(__file__))))
        try:
            upgrader.metadata_upgrade()
        except:
            pass
        upgrader.config_path_tempdir = config_path_tempdir

    def test_rollback_success(self):
        upgrader = Upgrader(dbmind_path, dbmind_conf)
        upgrader.new_version = upgrader.get_new_version()
        # upgrader.has_upgrade_config_error is True
        upgrader.config_upgrade = MagicMock(side_effect=FileNotFoundError('config path is not found'))
        upgrader.check_config_path = MagicMock(return_value=True)
        with self.assertRaises(FileNotFoundError):
            upgrader.upgrade()
        upgrader.cleanup()

        upgrader = Upgrader(dbmind_path, dbmind_conf)
        try_check_config_path(upgrader)
        upgrader.old_version = get_config_version(upgrader.config_path)
        upgrader.backup_config_path()
        upgrader.new_version = upgrader.get_new_version()
        upgrader.new_version = '5.2.0'
        assert upgrader.old_version == '5.1.0'

    def test_commit(self):
        upgrader = Upgrader(dbmind_path, dbmind_conf)
        conn_mock = MagicMock()
        conn_mock.commit.return_value = True
        upgrader.database_conns = [conn_mock]
        upgrader.cleanup()