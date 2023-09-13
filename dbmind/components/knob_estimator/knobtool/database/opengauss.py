import time
import logging

from ..utils import ExecutionError
from dbmind.common.cmd_executor import ExecutorFactory
from dbmind.common.parser.others import to_tuples


class GaussDB:
    def __init__(
        self,
        host,
        host_user,
        host_user_passwd,
        db_name,
        db_user,
        db_passwd,
        gauss_home,
        db_port=5432,
        ssh_port=22,
    ) -> None:
        self.ssh = (
            ExecutorFactory()
            .set_host(host)
            .set_user(host_user)
            .set_pwd(host_user_passwd)
            .set_port(ssh_port)
            .get_executor()
        )
        self.knobs = None
        self.host = host
        self.host_user = host_user
        self.host_user_passwd = host_user_passwd
        self.ssh_port = ssh_port
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.cmd_prefix = f"export GAUSSHOME={gauss_home} && PATH=$GAUSSHOME/bin:$PATH && export LD_LIBRARY_PATH=$GAUSSHOME/lib:$LD_LIBRARY_PATH && "
        self.data_path = self.get_data_path()
        self.create_database_if_not_exists(self.db_name)

    def _exec_statement(self, sql, timeout=None, db_name=None):
        command = 'gsql -p {db_port} -U {db_user} -d {db_name} -W {db_user_pwd} -c "{sql}";'.format(
            db_port=self.db_port,
            db_user=self.db_user,
            db_name=self.db_name if db_name is None else db_name,
            db_user_pwd=self.db_passwd,
            sql=sql,
        )
        stdout, stderr = self.ssh.exec_command_sync(self.cmd_prefix + command, timeout)
        if len(stderr) > 0 or self.ssh.exit_status != 0:
            logging.error(
                "Cannot execute SQL statement: %s. Error message: %s.", sql, stderr
            )
            raise ExecutionError("Cannot execute SQL statement: %s." % sql)

        return to_tuples(stdout)

    def _update_config(self, config_results):
        # candidates sort
        candidates = [ci["name"] for ci in self.knobs.candidates_info]
        config_list = []
        for name in candidates:
            config_list.append(
                (
                    name,
                    str(config_results[name])
                    + [ki for ki in self.knobs.knobs_info if ki["name"] == name][0][
                        "unit"
                    ],
                )
            )
        for knob, value in config_list:
            self._set_knob_value(knob, value)

    def _set_knob_value(self, name, value):
        logging.info("change knob: [%s=%s]", name, value)
        try:
            self.exec_command_on_host(
                'gs_guc reload -c "%s=%s" -D %s' % (name, value, self.data_path)
            )
        except ExecutionError as e:
            if str(e).find("Success to perform gs_guc!") < 0:
                logging.warning(e)

    def get_knobs_value(self, knobs):
        knob_value = {}
        for knob in knobs:
            res = self._exec_statement(
                f"select setting from pg_settings where name='{knob}'"
            )
            knob_value[knob] = res[0][0]
        return knob_value

    def update(self, sample, is_restart=False):
        self._update_config(sample)
        if is_restart:
            self.restart()

    def exec_command_on_host(self, cmd, timeout=None, ignore_status_code=False):
        stdout, stderr = self.ssh.exec_command_sync(
            self.cmd_prefix + cmd, timeout=timeout
        )

        if len(stderr) > 0 or self.ssh.exit_status != 0:
            error_msg = (
                "An error occurred when executing the command '%s'. "
                "The error information is: %s, the output information is %s."
                % (cmd, stderr, stdout)
            )
            if ignore_status_code and not (
                len(stderr) > 0 and self.ssh.exit_status != 0
            ):
                logging.warning(error_msg)
            else:
                raise ExecutionError(error_msg)
        return stdout

    def create_database_if_not_exists(self, database):
        res = self._exec_statement(
            "SELECT datname FROM pg_catalog.pg_database WHERE datname = '{}';".format(
                database
            ),
            db_name="postgres",
        )
        if not res:
            self._exec_statement(
                "CREATE DATABASE {};".format(database),
                db_name="postgres",
            )

    def is_alive(self):
        try:
            stdout = self.exec_command_on_host("ps -ux | grep gaussdb | wc -l")
            at_least_count = 1
            if int(stdout.strip()) <= at_least_count:
                return False
        except ExecutionError:
            return False

        sql = "SELECT now();"
        try:
            self._exec_statement(sql, db_name="postgres", timeout=1)
            return True
        except ExecutionError:
            return False

    def sample_history_queries(self, num, time, operator):
        res = self._exec_statement(
            f"select query {', query_plan' if operator else ''} from statement_history where start_time > \
                now() - interval '{time} minutes' limit {num};",
            db_name="postgres",
        )
        return res

    def get_data_path(self):
        if not self.is_alive():
            raise ExecutionError(
                "Failed to login to the database. "
                "Check whether the database is started. "
            )

        # Get database instance pid and data_path.
        return self._exec_statement(
            "SELECT datapath FROM pg_node_env;", db_name="postgres"
        )[0][0]

    def close(self):
        self.ssh.close()

    def restart(self):
        logging.info("Restarting database.")
        try:
            self._exec_statement("checkpoint;")
        except ExecutionError:
            logging.warning("Cannot checkpoint perhaps due to bad GUC settings.")
        self.exec_command_on_host(
            "gs_ctl stop -D {data_path}".format(data_path=self.data_path),
            ignore_status_code=True,
        )
        self.exec_command_on_host(
            "gs_ctl start -D {data_path}".format(data_path=self.data_path),
            ignore_status_code=True,
        )

        if self.is_alive():
            logging.info("The database restarted successfully.")
        else:
            logging.fatal("The database restarted failed.")
            raise ExecutionError("The database restarted failed.")
