# Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import psycopg2.errors
import psycopg2.extensions
import psycopg2.extras
import psycopg2.pool
import sqlparse

from dbmind.common.utils import dbmind_assert, write_to_terminal

_psycopg2_kwargs = dict(
    options="-c session_timeout=15 -c search_path=public",
    application_name='DBMind-openGauss-exporter',
    sslmode='disable',
)


def psycopg2_connect(dsn):
    # Set the session_timeout to prevent connection leak.
    conn = psycopg2.connect(
        dsn, **_psycopg2_kwargs
    )
    return conn


class ConnectionPool(psycopg2.pool.ThreadedConnectionPool):
    """A connection pool that works with the threading module and waits for getting a
    new connection rather than raise an exception while the pool is exhausted."""

    MAX_RETRY_TIME = 1  # second
    WAIT_TICK = 0.1  # second

    def _getconn(self, key=None):
        max_retry_times = ConnectionPool.MAX_RETRY_TIME / ConnectionPool.WAIT_TICK
        n_retry = 1
        while len(self._used) == self.maxconn and n_retry < max_retry_times:
            time.sleep(0.1)
            n_retry += 1

        # Response is first.
        if len(self._used) == self.maxconn:
            if key is None:
                key = self._getkey()
            return self._connect(key)

        return super()._getconn(key)


class Driver:
    def __init__(self):
        self._url = None
        self.parsed_dsn = None
        self.initialized = False
        self._pool = None

    def initialize(self, url, pool_size=None):
        """
        :param url: connect to database by using this url (or DSN).
        :param pool_size: connection pool size, if set None, this calls doesn't use pool.
        :return: Nothing to return
        """
        try:
            # Specify default schema is public.
            conn = psycopg2_connect(url)
            conn.cursor().execute('select 1;')
            conn.close()
            self._url = url
            self.parsed_dsn = psycopg2.extensions.parse_dsn(url)
            if pool_size:
                self._pool = ConnectionPool(
                    minconn=1, maxconn=pool_size,
                    dsn=url, **_psycopg2_kwargs
                )
            self.initialized = True
        except Exception as e:
            raise ConnectionError(e)

    @property
    def address(self):
        return '%s:%s' % (self.parsed_dsn['host'], self.parsed_dsn['port'])

    @property
    def host(self):
        return self.parsed_dsn['host']

    @property
    def port(self):
        return self.parsed_dsn['port']

    @property
    def dbname(self):
        return self.parsed_dsn['dbname']

    @property
    def username(self):
        return self.parsed_dsn['user']

    @property
    def pwd(self):
        return self.parsed_dsn['password']

    def query(self, stmt, timeout=0, force_connection_db=None,
              return_tuples=False, fetch_all=False, ignore_error=False):
        dbmind_assert(self.initialized)

        cursor_dict = {}
        if not return_tuples:
            cursor_dict['cursor_factory'] = psycopg2.extras.RealDictCursor
        try:
            conn = self.get_conn(force_connection_db)
            with conn.cursor(
                    **cursor_dict
            ) as cursor:
                try:
                    start = time.monotonic()
                    if timeout > 0:
                        cursor.execute('SET statement_timeout = %d;' % (timeout * 1000))
                    if not fetch_all:
                        cursor.execute(stmt)
                        result = cursor.fetchall()
                    else:
                        result = []
                        for sql in sqlparse.split(stmt):
                            if ignore_error:
                                try:
                                    cursor.execute(sql)
                                except Exception as e:
                                    result.append(None)
                                    continue
                                finally:
                                    conn.commit()
                            else:
                                cursor.execute(sql)
                            if cursor.pgresult_ptr is not None:
                                result.append(cursor.fetchall())
                            else:
                                result.append(None)
                    conn.commit()
                except psycopg2.extensions.QueryCanceledError as e:
                    logging.error('%s: %s.' % (e.pgerror, stmt))
                    logging.info(
                        'Time elapsed during execution is %fs '
                        'but threshold is %fs.' % (time.monotonic() - start, timeout)
                    )
                    result = []
                except psycopg2.errors.FeatureNotSupported:
                    logging.warning('FeatureNotSupported while executing %s.', stmt)
                    result = []
                except psycopg2.errors.ObjectNotInPrerequisiteState:
                    logging.warning('ObjectNotInPrerequisiteState while executing %s.', stmt)
                    result = []
                except psycopg2.errors.UndefinedParameter:
                    logging.warning('UndefinedParameter while executing %s.', stmt)
                    result = []
                except psycopg2.errors.UndefinedColumn:
                    logging.warning('UndefinedColumn while executing %s.', stmt)
                    result = []
            self.put_conn(conn)
        except psycopg2.InternalError as e:
            logging.error("Cannot execute '%s' due to internal error: %s." % (stmt, e.pgerror))
            result = []
        except Exception as e:
            logging.exception(e)
            result = []
        return result

    def get_conn(self, force_connection_db=None):
        """Cache the connection in the thread so that the thread can
        reuse this connection next time, thereby avoiding repeated creation.
        By this way, we can realize thread-safe database query,
        and at the same time, it can also have an ability similar to a connection pool. """
        # If query wants to connect to other database by force, we can generate and cache
        # the new connection as the following.
        if force_connection_db:
            parsed_dsn = self.parsed_dsn.copy()
            parsed_dsn['dbname'] = force_connection_db
            dsn = ' '.join(['{}={}'.format(k, v) for k, v in parsed_dsn.items()])
            # We don't cache the connection that uses force_connection_db because
            # this scenario is not frequent and we also don't advise users use this
            # mode usually in the code. If users have to connect to multiple database,
            # they should use the DriverBundle instead.
            return psycopg2_connect(dsn)

        # If not used connection pool, create and return a new connection directly.
        if not self._pool:
            return psycopg2_connect(dsn=self._url)

        conn = self._pool.getconn()
        # Check whether the connection is timeout or invalid.
        try:
            conn.cursor().execute('select 1;')
        except (
                psycopg2.InternalError,
                psycopg2.InterfaceError,
                psycopg2.errors.AdminShutdown,
                psycopg2.OperationalError
        ) as e:
            logging.warning(
                'Cached database connection to openGauss'
                ' has been timeout due to %s.' % e
            )
            self._pool.putconn(conn, close=True)
        except Exception as e:
            logging.error('Failed to connect to openGauss '
                          'with cached connection (%s).' % e)
            self._pool.putconn(conn, close=True)

        return conn

    def put_conn(self, conn, close=False):
        """Put away a connection."""
        dbname = psycopg2.extensions.parse_dsn(conn.dsn)['dbname']
        if not self._pool or dbname != self.dbname:
            # If not used connection pool or not main dbname,
            # close the connection directly.
            conn.close()
            return
        self._pool.putconn(conn, close=close)


class DriverBundle:
    __main_db_name__ = 'postgres'

    UPDATE_PERIOD = 300  # seconds

    _thread_pool_executor = ThreadPoolExecutor(
        thread_name_prefix='DriverBundleWorker'
    )

    def __init__(
            self, url,
            include_db_list=None,
            exclude_db_list=None,
            each_db_max_connections=None,
            log_to_terminal=True
    ):
        # this bundle is to maintain all database connections,
        # which will be updated by `self.update()`
        self.main_driver = Driver()
        # If cannot access, raise a ConnectionError.
        self.main_driver.initialize(url, each_db_max_connections)

        if self.main_dbname != DriverBundle.__main_db_name__:
            msg = (
                'The default connection database of the exporter is not postgres, '
                'so it is possible that some database metric information '
                'cannot be collected, such as slow SQL queries.'
            )
            logging.warning(msg)
            if log_to_terminal:
                write_to_terminal(msg)

        if not self.guarantee_access():
            msg = (
                'The current user does not have the Monitoradmin/Sysadmin privilege, '
                'which will cause many metrics to fail to obtain. '
                'Please consider granting this privilege to the connecting user.'
            )
            logging.warning(msg)
            if log_to_terminal:
                write_to_terminal(msg)

        # add other database connections to the bundle
        self._bundle = dict()
        self._include_db_list = include_db_list
        self._exclude_db_list = exclude_db_list
        self._each_db_max_connections = each_db_max_connections
        self._last_updated = 0
        self._update_lock = threading.RLock()
        self.update()

    def update(self):
        # update bundle databases if timed out.
        last_updated, self._last_updated = self._last_updated, time.monotonic()
        if time.monotonic() - last_updated < self.UPDATE_PERIOD:
            # if not timed out, we don't need to update.
            return
        with self._update_lock:
            # clean items if exists
            self._bundle.clear()
            self._bundle = {self.main_driver.dbname: self.main_driver}
            for dbname in self._discover_databases(self._include_db_list, self._exclude_db_list):
                if dbname in self._bundle:
                    continue

                try:
                    driver = Driver()
                    driver.initialize(self._splice_url_for_other_db(dbname),
                                      self._each_db_max_connections)
                    # Ensure that each driver can access corresponding database.
                    self._bundle[dbname] = driver
                except ConnectionError:
                    logging.warning(
                        'Cannot connect to the database %s by using the given user.', dbname
                    )

    def _discover_databases(self, include_dbs, exclude_dbs):
        if not include_dbs:
            include_dbs = {}
        if not exclude_dbs:
            exclude_dbs = {}
        include_dbs = set(include_dbs)
        exclude_dbs = set(exclude_dbs)

        # We cannot allow to both set these two arguments.
        dbmind_assert(not (include_dbs and exclude_dbs))

        all_db_list = self.main_driver.query(
            'SELECT datname FROM pg_catalog.pg_database;',
            return_tuples=True
        )
        discovered = set()
        for dbname in all_db_list:
            if dbname[0] in ('template0', 'template1'):  # Skip these useless databases.
                continue
            discovered.add(dbname[0])

        if include_dbs:
            return discovered.intersection(include_dbs)

        return discovered - exclude_dbs

    def _splice_url_for_other_db(self, dbname):
        parsed_dsn = self.main_driver.parsed_dsn.copy()
        parsed_dsn['dbname'] = dbname
        return ' '.join(['{}={}'.format(k, v) for (k, v) in parsed_dsn.items()])

    def query(self, stmt, timeout=0, force_connection_db=None, return_tuples=False):
        """A decorator for Driver.query. If the caller sets
        the parameter `force_connection_db`, this method only returns
        the query result from this specified database.
        Otherwise, the method will return the
        union set of each database's execution result.

        This method need to guaranteed thread safety.
        """
        # update database connections
        self.update()

        if force_connection_db is not None:
            if force_connection_db not in self._bundle:
                return []
            return self._bundle[force_connection_db].query(stmt, timeout, None, return_tuples)

        # Use multiple threads to query.
        futures = []
        for dbname in self._bundle:
            driver = self._bundle[dbname]
            futures.append(
                DriverBundle._thread_pool_executor.submit(
                    driver.query, stmt, timeout, None, return_tuples
                )
            )

        union_set = set()
        for future in as_completed(futures):
            try:
                # Because the driver's execution result is a 2-dimension array,
                # we need to take the array apart then combine them.
                result = future.result()
                for row in result:
                    if return_tuples:
                        union_set.add(tuple(row))
                    else:
                        union_set.add(tuple(row.items()))
            except Exception as e:
                logging.exception(e)
        if return_tuples:
            return list(union_set)
        else:
            # Get back to the dict-based format.
            ret = []
            for row in union_set:
                dict_based_row = {}
                for k, v in row:
                    dict_based_row[k] = v
                ret.append(dict_based_row)
            return ret

    @property
    def address(self):
        return self.main_driver.address

    @property
    def host(self):
        return self.main_driver.host

    @property
    def port(self):
        return self.main_driver.port

    @property
    def main_dbname(self):
        return self.main_driver.dbname

    @property
    def username(self):
        return self.main_driver.username

    def is_monitor_admin(self):
        r = self.main_driver.query(
            'select rolmonitoradmin from pg_roles where rolname = CURRENT_USER;',
            return_tuples=True
        )
        return r[0][0]

    def is_system_admin(self):
        """test if the current user is the system user."""
        res = self.main_driver.query(
            'select rolsystemadmin from pg_roles where rolname = CURRENT_USER;',
            return_tuples=True
        )
        return res[0][0]

    def guarantee_access(self):
        if self.is_monitor_admin():
            return True
        if self.is_system_admin():
            # although we have sysadmin privileges,
            # the tables under dbe_perf are still not
            # accessible, so we need to self-grant.
            self.main_driver.query(
                f'ALTER USER {self.username} monadmin;',
                return_tuples=True,
                fetch_all=True,
                ignore_error=True
            )
            return self.is_monitor_admin()
        return False

    def is_standby(self):
        r = self.main_driver.query(
            'select pg_catalog.pg_is_in_recovery();',
            return_tuples=True
        )
        return r[0][0]

