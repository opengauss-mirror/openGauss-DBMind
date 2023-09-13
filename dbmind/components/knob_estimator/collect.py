import os
import logging

from .rank import create_rank_info
from .knobtool import constants as my_constants
from .knobtool.workload import benchbase
from .knobtool.collector import Collector
from .knobtool.knobs_manager import Knobs
from .knobtool.database.opengauss import GaussDB


def collect(config, two_stage=False, result_path=None, candidates=None, size=None):
    if candidates is None:
        candidates = config["knob_candidates"]

    dbms = GaussDB(
        host=my_constants.CM_HOST,
        host_user=my_constants.CM_HOST_USER,
        host_user_passwd=my_constants.CM_HOST_USER_PASSWD,
        db_name=my_constants.DB_NAME,
        db_user=my_constants.DB_USER,
        db_passwd=my_constants.DB_PASSWD,
        db_port=my_constants.DB_PORT,
        ssh_port=my_constants.CM_SSH_PORT,
        gauss_home=my_constants.GAUSSHOME,
    )

    bak_config = dbms.get_knobs_value(candidates)

    dbms.knobs = Knobs(
        knobs_csv_path=my_constants.DB_KNOBS_INFO,
        candidates=candidates,
    )
    workload = benchbase.Benchbase(args=config["workload"])

    if two_stage:
        logging.info("collector stage one start")
        data_file, _ = _collect(
            dbms=dbms,
            workload=workload,
            size=20,
            result_path=os.path.join(
                config["file_dir"],
                "collect_tmp.csv",
            ),
        )
        rank_res = create_rank_info(data_file).rank
        logging.info("Knob rank result: " + str(rank_res))
        dbms.update(bak_config)
        dbms.knobs = Knobs(
            knobs_csv_path=my_constants.DB_KNOBS_INFO,
            candidates=[_ for _ in rank_res.keys()][:6],
        )
        os.remove(data_file)

    logging.info("collector start")
    data_file, total_list = _collect(
        dbms=dbms,
        workload=workload,
        size=(config["size"] if size is None else size),
        result_path=result_path,
    )
    dbms.update(bak_config)
    return data_file, total_list


def _collect(dbms, workload, size, result_path, sample_policy="lhs", seed=100):
    col = Collector(
        database=dbms,
        workload=workload,
        sample_policy=sample_policy,
        metric="through",
        seed=seed,
    )
    res = col.execute(num=size, result_path=result_path)
    return res
