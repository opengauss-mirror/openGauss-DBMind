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
import datetime

from dbmind.common.utils import dbmind_assert
from dbmind.common.utils.checking import split_ip_port
from dbmind.service.web.context_manager import ACCESS_CONTEXT_NAME, get_access_context


def sqlalchemy_query_jsonify(query, field_names=None):
    rv = {'header': field_names, 'rows': []}
    if not field_names:
        field_names = query.statement.columns.keys()  # in order keys.
    rv['header'] = field_names
    for result in query:
        if hasattr(result, '__iter__'):
            row = list(result)
        else:
            row = [getattr(result, field) for field in field_names]
        rv['rows'].append(row)
    return rv


def sqlalchemy_query_jsonify_for_multiple_instances(
        query_function, instances, **kwargs
):
    field_names = kwargs.pop('field_names', None)
    if not instances:
        return sqlalchemy_query_jsonify(query_function(**kwargs), field_names=field_names)
    rv = None
    for instance in instances:
        r = sqlalchemy_query_jsonify(query_function(instance=instance, **kwargs), field_names=field_names)
        if not rv:
            rv = r
        else:
            rv['rows'].extend(r['rows'])
    return rv


def psycopg2_dict_jsonify(realdict, field_names=None):
    rv = {'header': field_names, 'rows': []}
    if len(realdict) == 0:
        return rv

    if not field_names:
        rv['header'] = list(realdict[0].keys())
    for obj in realdict:
        old_date_key = 'last_updated'
        if old_date_key in obj.keys():
            obj[old_date_key] = format_date_key(obj, old_date_key)
        row = []
        for field in rv['header']:
            row.append(obj[field])
        rv['rows'].append(row)
    return rv


def _sqlalchemy_query_records_logic(query_function, instances, **kwargs):
    if instances is None or len(instances) != 1:
        r1 = sqlalchemy_query_jsonify_for_multiple_instances(
            query_function=query_function,
            instances=get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST),
            **kwargs
        )
        r2 = sqlalchemy_query_jsonify_for_multiple_instances(
            query_function=query_function,
            instances=get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST),
            **kwargs
        )
        r1['rows'].extend(r2['rows'])
        return r1

    instance = instances[0]
    ip, port = split_ip_and_port(instance)
    if port is None:
        if ip not in get_access_context(
                ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST
        ):
            # return nothing
            return sqlalchemy_query_jsonify(query_function(instance='', **kwargs))
    else:
        if instance not in get_access_context(
                ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST
        ):
            # return nothing
            return sqlalchemy_query_jsonify(query_function(instance='', **kwargs))
    r1 = sqlalchemy_query_jsonify(
        query_function(ip, **kwargs)
    )
    if port is None:
        return r1
    r2 = sqlalchemy_query_jsonify(
        query_function(instance, **kwargs)
    )
    r1['rows'].extend(r2['rows'])
    return r1


def sqlalchemy_query_records_count_logic(count_function, instances, **kwargs):
    result = 0
    only_with_port = kwargs.pop('only_with_port', False)
    if instances is None or len(instances) != 1:
        if only_with_port:
            instances = get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
        else:
            instances = get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST) + get_access_context(
                ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
        for instance in instances:
            result += count_function(instance, **kwargs)
        return result

    instance = instances[0]
    result = count_function(instance, **kwargs)
    return result


def sqlalchemy_query_union_records_logic(query_function, instances, **kwargs):
    only_with_port = kwargs.pop('only_with_port', False)
    offset = kwargs.pop('offset', None)
    limit = kwargs.pop('limit', None)
    field_names = None
    if instances is None or len(instances) != 1:
        if only_with_port:
            instances = get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
        else:
            instances = get_access_context(ACCESS_CONTEXT_NAME.INSTANCE_IP_LIST) + get_access_context(
                ACCESS_CONTEXT_NAME.INSTANCE_IP_WITH_PORT_LIST)
        # Notice: the following `query_function` allows to receive a list or tuple
        # then use using clause for predicate, which all have adapted for this function.
        r = query_function(instance=instances, **kwargs)
        field_names = r.statement.columns.keys()
    else:
        dbmind_assert(len(instances) == 1,
                      'Found code bug: the variable instances cannot '
                      'have zero or one more elements.')
        instance = instances[0]
        r = query_function(instance, **kwargs)
    if r is not None:
        if offset is not None:
            r = r.offset(offset)
        if limit is not None:
            r = r.limit(limit)
        return sqlalchemy_query_jsonify(r, field_names)
    return sqlalchemy_query_jsonify(query_function(instance='', **kwargs))


def format_date_key(obj, old_date_key):
    new_date = obj[old_date_key]
    if obj[old_date_key].find('.') >= 0 and obj[old_date_key].find('+00:00') >= 0:
        new_date = datetime.datetime.strptime(obj[old_date_key], "%Y-%m-%d %H:%M:%S.%f+00:00").strftime(
            '%Y-%m-%d %H:%M:%S')
    return new_date


def split_ip_and_port(address):
    if not address:
        return None, None
    arr = split_ip_port(address)
    if len(arr) < 2:
        return arr[0], None
    return arr[0], arr[1]
