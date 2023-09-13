from . import constants as my_constants
from .database.opengauss import GaussDB


class ExecutionError(Exception):
    pass


def get_feature_from_DB(normalize=False, num=100, time=30, operator=False):
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
    suid_feature = {"INSERT": 0, "SELECT": 0, "DELETE": 0, "UPDATE": 0}
    opts_feature = {
        "Seq Scan": 0,
        "Index Scan": 0,
        "Bitmap Index Scan": 0,
        "Bitmap Heap Scan": 0,
        "Hash Join": 0,
        "Sort": 0,
        "Merge Join": 0,
        "Nested Loop": 0,
        "Index Only Scan": 0,
        "Aggregate": 0,
        "Hash": 0,
        "Gather": 0,
    }
    queries = dbms.sample_history_queries(num=num, time=time, operator=operator)
    for query in queries:
        is_query = False
        for suid_key in suid_feature.keys():
            if suid_key in query[0]:
                is_query = True
                suid_feature[suid_key] = suid_feature[suid_key] + 1
        if not is_query:
            continue
        for opts_key in opts_feature:
            if opts_key in query[1]:
                opts_feature[opts_key] = opts_feature[opts_key] + 1

    if normalize:
        suid_sum = sum(suid_feature.values())
        opts_sum = sum(opts_feature.values())
        for key in suid_feature.keys():
            suid_feature[key] = suid_feature[key] / suid_sum
        for key in opts_feature.keys():
            opts_feature[key] = opts_feature[key] / opts_sum
    return suid_feature, opts_feature
