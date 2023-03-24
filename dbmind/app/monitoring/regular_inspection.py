# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
import re
from datetime import datetime, timedelta
from collections import defaultdict

from dbmind import global_vars
from dbmind.common.algorithm.data_statistic import get_statistic_data, box_plot
from dbmind.metadatabase import dao
from dbmind.service import dai
from dbmind.service.web.jsonify_utils import \
    sqlalchemy_query_jsonify_for_multiple_instances as sqlalchemy_query_jsonify_for_multiple_instances
from dbmind.service.dai import is_sequence_valid

ONE_DAY = 24 * 60


def _get_root_cause(root_causes):
    if root_causes is None:
        return []
    return re.findall(r'\d{1,2}\. ([A-Z_]+): \(', root_causes)


def _get_query_type(query):
    query = query.upper()
    if 'SELECT' in query:
        return 'SELECT'
    elif 'UPDATE' in query:
        return 'UPDATE'
    elif 'DELETE' in query:
        return 'DELETE'
    elif 'INSERT' in query:
        return 'INSERT'
    else:
        return 'OTHER'


def get_metric_statistic_threshold(instance, metric, latest_minutes=ONE_DAY, **kwargs):
    # automatic calculation of parameter thresholds.
    # note： it is used for unimportant metric.
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=latest_minutes)
    sequence = dai.get_metric_sequence(metric, start_time, end_time).from_server(instance).filter(**kwargs).fetchone()
    if not is_sequence_valid(sequence):
        return
    upper, lower = box_plot(sequence.values, n=1.5)
    return upper, lower


def get_sequence_value(s, func=None):
    if is_sequence_valid(s):
        if callable(func):
            return func(s.values)
        return s.values[-1]
    return 0


class DailyInspection:
    def __init__(self, instance, start=None, end=None):
        self._report = {}
        self._start = start
        self._end = end
        self._agent_instance = instance
        all_agents = global_vars.agent_proxy.agent_get_all()
        self._instances_with_port = all_agents.get(instance)
        self._instances_with_no_port = [i.split(':')[0] for i in self._instances_with_port]

    @property
    def resource(self):
        rv = {'header': ('metric', 'instance', 'max', 'min', 'avg', 'the_95th'), 'rows': []}
        metrics = ('os_cpu_usage', 'os_mem_usage', 'os_disk_ioutils', 'os_disk_usage')
        for instance in self._instances_with_no_port:
            for metric in metrics:
                sequence = dai.get_metric_sequence(metric, self._start, self._end).from_server(instance).fetchone()
                if is_sequence_valid(sequence):
                    avg_val, min_val, max_val, the_95th_val = get_statistic_data(sequence.values)
                    rv['rows'].append((metric, instance, max_val, min_val, avg_val, the_95th_val))
        return rv

    @property
    def dml(self):
        select_sequence = dai.get_metric_sequence('pg_sql_count_select', self._start, self._end).\
            from_server(self._agent_instance).fetchone()
        update_sequence = dai.get_metric_sequence('pg_sql_count_update', self._start, self._end).\
            from_server(self._agent_instance).fetchone()
        insert_sequence = dai.get_metric_sequence('pg_sql_count_insert', self._start, self._end).\
            from_server(self._agent_instance).fetchone()
        delete_sequence = dai.get_metric_sequence('pg_sql_count_delete', self._start, self._end).\
            from_server(self._agent_instance).fetchone()
        dml_distribution = {'select': get_sequence_value(select_sequence, max),
                            'delete': get_sequence_value(delete_sequence, max),
                            'update': get_sequence_value(update_sequence, max),
                            'insert': get_sequence_value(insert_sequence, max)}
        return dml_distribution

    @property
    def performance(self):
        performance_detail = {}
        tps_sequence = dai.get_metric_sequence('gaussdb_qps_by_instance', self._start, self._end).\
            from_server(self._agent_instance).fetchone()
        p95_sequence = dai.get_metric_sequence('statement_responsetime_percentile_p95',
                                           self._start, self._end).from_server(self._agent_instance).fetchone()
        if is_sequence_valid(tps_sequence):
            avg_val, min_val, max_val, the_95th_val = get_statistic_data(tps_sequence.values)
            performance_detail['tps'] = {'avg': avg_val, 'max': max_val, 'min': min_val, 'the_95th': the_95th_val}
        if is_sequence_valid(p95_sequence):
            avg_val, min_val, max_val, the_95th_val = get_statistic_data(tps_sequence.values)
            performance_detail['p95'] = {'avg': avg_val, 'max': max_val, 'min': min_val, 'the_95th': the_95th_val}
        return performance_detail

    @property
    def db_size(self):
        rv = {'header': ('dbname', 'max', 'min', 'is_continuous_increase'), 'rows': []}
        sequences = dai.get_metric_sequence('pg_database_size_bytes', self._start, self._end).\
            from_server(self._agent_instance).fetchall()
        for sequence in sequences:
            if is_sequence_valid(sequence):
                dbname = sequence.labels.get('datname', 'UNKNOWN')
                rv['rows'].append((dbname, 
                                   round(get_sequence_value(sequence, max) / 1024 / 1024, 2), 
                                   round(get_sequence_value(sequence, min) / 1024 / 1024, 2), 'False'))
        return rv

    @property
    def table_size(self):
        rv = {'header': ('dbname', 'schema', 'tablename', 'tablesize', 'indexsize'), 'rows': []}
        sequences = dai.get_metric_sequence('pg_tables_size_relsize', self._start, self._end).\
            from_server(self._agent_instance).fetchall()
        for sequence in sequences:
            if is_sequence_valid(sequence):
                schema = sequence.labels.get('nspname', 'UNKNOWN')
                relname = sequence.labels.get('relname', 'UNKNOWN')
                datname = sequence.labels.get('datname', 'UNKNOWN')
                indexsize_seq = dai.get_metric_sequence('pg_tables_size_indexsize', self._start, self._end).\
                    from_server(self._agent_instance).filter(nspname=schema, relname=relname).fetchone()
                rv['rows'].append((datname, schema, relname, round(get_sequence_value(sequence, max), 2),
                                   round(get_sequence_value(indexsize_seq, max), 2)))
        return rv

    @property
    def history_alarm(self):
        instances = self._instances_with_port + self._instances_with_no_port
        return sqlalchemy_query_jsonify_for_multiple_instances(
            query_function=dao.alarms.select_history_alarm,
            instances=instances,
            group=True
    )

    @property
    def future_alarm(self):
        instances = self._instances_with_port + self._instances_with_no_port
        return sqlalchemy_query_jsonify_for_multiple_instances(
            query_function=dao.alarms.select_future_alarm,
            instances=instances,
            group=True
    )


    @property
    def slow_sql_rca(self):
        query_type_recorder, query_template_recorder = {}, {}
        root_cause_distribution = {}
        query_type_distribution = {'header': ['dbname', 'select', 'delete', 'update', 'insert'], 'rows': []}
        query_templates = {'header': ['dbname', 'template', 'count'], 'rows': []}
        result = dao.slow_queries.select_slow_queries(instance=self._agent_instance,
                                                      start_time=dai.datetime_to_timestamp(self._start),
                                                      end_time=dai.datetime_to_timestamp(self._end),
                                                      group=True)
        for slow_query in result:
            query = getattr(slow_query, 'query')
            dbname = getattr(slow_query, 'db_name')
            root_causes = getattr(slow_query, 'root_cause')
            count = getattr(slow_query, 'count')
            query_type = _get_query_type(query)
            if dbname not in query_type_recorder:
                query_type_recorder[dbname] = {'select': 0, 'delete': 0, 'update': 0, 'insert': 0, 'other': 0}
            query_type_recorder[dbname][query_type.lower()] += count
            if dbname not in query_template_recorder:
                query_template_recorder[dbname] = {}
            if query not in query_template_recorder[dbname]: 
                query_template_recorder[dbname][query] = 0
            query_template_recorder[dbname][query] += count
            parse_root_causes = _get_root_cause(root_causes)
            for root_cause in parse_root_causes:
                if root_cause not in root_cause_distribution:
                    root_cause_distribution[str(root_cause)] = 0
                root_cause_distribution[root_cause] += count
        for dbname, query_type_count in query_type_recorder.items():
            query_type_distribution['rows'].append([dbname, query_type_count['select'],
                                                    query_type_count['delete'],
                                                    query_type_count['update'],
                                                    query_type_count['insert']])
        for dbname, template_detail in query_template_recorder.items():
            for template, count in template_detail.items():
                query_templates['rows'].append([dbname, template, count])
        return {"query_type_distribution": query_type_distribution, 
                "root_cause_distribution": root_cause_distribution, 
                "query_templates": query_templates}

    @property
    def connection(self):
        active_connection, total_connection = {}, {}
        active_conn_sequence = dai.get_metric_sequence('gaussdb_active_connection', self._start, self._end).\
            from_server(self._agent_instance).fetchone()
        total_conn_sequence = dai.get_metric_sequence('gaussdb_total_connection', self._start, self._end).\
            from_server(self._agent_instance).fetchone()
        if is_sequence_valid(active_conn_sequence):
            avg_val, min_val, max_val, the_95th_val = get_statistic_data(active_conn_sequence.values)
            active_connection = {'max': max_val, 'min': min_val, 'avg': avg_val, 'the_95th': the_95th_val}
        if is_sequence_valid(total_conn_sequence):
            avg_val, min_val, max_val, the_95th_val = get_statistic_data(total_conn_sequence.values)
            total_connection = {'max': max_val, 'min': min_val, 'avg': avg_val, 'the_95th': the_95th_val}
     
        return {'total_connection': total_connection ,'active_connection': active_connection}

    @property
    def dynamic_memory(self):
        dynamic_used_memory = defaultdict()
        for instance in self._instances_with_port:
            host, _ = instance.split(':')
            host_like = host + "(:[0-9]{4,5}|)"
            dynamic_memory_sequence = dai.get_metric_sequence('pg_total_memory_detail_mbytes', self._start, self._end).\
                from_server(instance).filter(type='dynamic_used_memory').fetchone()
            total_memory_sequence = dai.get_latest_metric_value('node_memory_MemTotal_bytes').\
                filter_like(instance=host_like).fetchone()
            if is_sequence_valid(total_memory_sequence) and is_sequence_valid(dynamic_memory_sequence): 
                # transfer bytes to unit 'MB'
                total_memory = get_sequence_value(total_memory_sequence) / 1024 / 1024
                memory_rate = [round(item / total_memory, 2) for item in dynamic_memory_sequence.values]
                avg_val, min_val, max_val, the_95th_val = get_statistic_data(dynamic_memory_sequence.values)
                dynamic_used_memory[instance] = {'statistic': {'max': max_val, 'min': min_val,
                                                               'avg': avg_val, 'the_95th': the_95th_val},
                                                 'timestamps': dynamic_memory_sequence.timestamps,
                                                 'data': memory_rate}
        return dynamic_used_memory
    
    def __call__(self):
        return {'resource': self.resource,
                'dml': self.dml,
                'db_size': self.db_size,
                'table_size': self.table_size,
                'history_alarm': self.history_alarm,
                'future_alarm': self.future_alarm,  
                'slow_sql_rca': self.slow_sql_rca,
                'connection': self.connection, 
                'dynamic_memory': self.dynamic_memory, 
                'performance': self.performance}


def _get_regular_inspection_report(instance, inspection_type, start, end, limit):
    reports = sqlalchemy_query_jsonify_for_multiple_instances(
        query_function=dao.regular_inspections.select_metric_regular_inspections,
        instances=[instance],
        start=start, end=end, inspection_type=inspection_type, limit=limit)
    return reports


def _get_result_from_rv(rv, key):
    index_of_report = rv['header'].index('report')
    index_of_start = rv['header'].index('start')
    rv['rows'].sort(key=lambda x: x[index_of_start])
    value = [row[index_of_report].get(key) for row in rv['rows']]
    return value
 

def _get_time_from_rv(rv, key):
    if key == 'start':
        index = rv['header'].index('start')
    else:
        index = rv['header'].index('end')
    return sorted([item[index] for item in rv['rows']])


class MultipleDaysInspection:
    def __init__(self, instance, start, end, history_inspection_limit=3):
        self._report = {}
        self._history_inspection_limit = history_inspection_limit
        # transform datetime to timestamps
        self._start = int(start.timestamp() * 1000)
        self._end = int(end.timestamp() * 1000)
        self._agent_instance = instance
        all_agents = global_vars.agent_proxy.agent_get_all()
        self._instances_with_port = all_agents.get(instance)
        self._instances_with_no_port = [i.split(':')[0] for i in self._instances_with_port]
        self._existing_data = True
        self._history_report = _get_regular_inspection_report(instance, 'daily_check',
                                                              self._start, self._end, self._history_inspection_limit)
        self._history_start_time = _get_time_from_rv(self._history_report, 'start')

    @property
    def resource(self):
        instance_resource = {}
        resources = _get_result_from_rv(self._history_report, 'resource')  # [r1, r2, ...]
        for resource in resources:
            if not resource:
                continue
            rows = resource['rows']
            header = resource['header']
            metric_index, instance_index = header.index('metric'), header.index('instance')
            max_index, min_index = header.index('max'), header.index('min')
            avg_index, p95_index = header.index('avg'), header.index('the_95th')
            for row in rows:
                metric = row[metric_index]
                instance = row[instance_index]
                if instance not in instance_resource:
                    instance_resource[instance] = {}
                if metric not in instance_resource[instance]:
                    instance_resource[instance][metric] = {'max': [], 'min': [], 'avg': [], 'the_95th': []}
                instance_resource[instance][metric]['max'].append(row[max_index])
                instance_resource[instance][metric]['min'].append(row[min_index])
                instance_resource[instance][metric]['avg'].append(row[avg_index])
                instance_resource[instance][metric]['the_95th'].append(row[p95_index])
        return {'timestamps': self._history_start_time, 'data': instance_resource}

    @property
    def performance(self):
        instance_performance = {}
        performances = _get_result_from_rv(self._history_report, 'performance')
        for performance in performances:
            for metric, value in performance.items():
                if metric not in instance_performance:
                    instance_performance[metric] = {'max': [], 'min': [], 'avg': [], 'the_95th': []}
                instance_performance[metric]['max'].append(value['max'])
                instance_performance[metric]['min'].append(value['min'])
                instance_performance[metric]['avg'].append(value['avg'])
                instance_performance[metric]['the_95th'].append(value['the_95th'])
        return {'timestamp': self._history_start_time, 'data': instance_performance}

    @property
    def db_size(self):
        instance_db_size = {}
        db_sizes = _get_result_from_rv(self._history_report, 'db_size')
        for db_size in db_sizes:
            if not db_size:
                continue
            rows = db_size['rows']
            header = db_size['header']
            dbname_index = header.index('dbname')
            max_val_index = header.index('max')
            min_val_index = header.index('min')
            for row in rows:
                dbname = row[dbname_index]
                if dbname not in instance_db_size:
                    instance_db_size[dbname] = {'max': [], 'min': []}
                instance_db_size[dbname]['max'].append(row[max_val_index])
                instance_db_size[dbname]['min'].append(row[min_val_index])
        return {'timestamps': self._history_start_time, 'data': instance_db_size}

    @property
    def table_size(self):
        # output the fastest growing topk table
        instance_table_size = {}
        table_size_recorder = {}
        topk_table = 10
        table_sizes = _get_result_from_rv(self._history_report, 'table_size')
        for table_size in table_sizes:
            rows = table_size['rows']
            header = table_size['header']
            dbname_index = header.index('dbname')
            schema_index = header.index('schema')
            tablename_index = header.index('tablename')
            table_size_index = header.index('tablesize')
            index_size_index = header.index('indexsize')
            for row in rows:
                dbname = row[dbname_index]
                schema = row[schema_index]
                tablename = row[tablename_index]
                table_size = row[table_size_index]
                index_size = row[index_size_index]
                key = "%s-%s-%s" % (dbname, schema, tablename)
                if key not in table_size_recorder:
                    table_size_recorder[key] = []
                table_size_recorder[key].append((table_size, index_size, table_size + index_size))
        table_size_recorder = sorted(table_size_recorder.items(),
                                     key=lambda item: abs(max(item[1][-1]) - min(item[1][-1])), reverse=True)
        del(table_size_recorder[topk_table:])
        table_size_recorder = dict(table_size_recorder)
        for key, value in table_size_recorder.items():
            dbname, schema, table = key.split('-')
            if dbname not in instance_table_size:
                instance_table_size[dbname] = {}
            if schema not in instance_table_size[dbname]:
                instance_table_size[dbname][schema] = {}
            if table not in instance_table_size[dbname][schema]:
                instance_table_size[dbname][schema][table] = {'table_size': [], 'index_size': []}
            instance_table_size[dbname][schema][table]['table_size'] = [item[0] for item in value]
            instance_table_size[dbname][schema][table]['index_size'] = [item[1] for item in value]
        return {'timestamp': self._history_start_time, 'data': instance_table_size} 

    @property
    def history_alarm(self):
        # get history alarms directly from the metadatabase
        instances = self._instances_with_port + self._instances_with_no_port
        return sqlalchemy_query_jsonify_for_multiple_instances(
            query_function=dao.alarms.select_history_alarm,
            instances=instances, start_at=self._start, end_at=self._end, 
            group=True)

    @property
    def future_alarm(self):
        # get history alarms directly from the metadatabase
        instances = self._instances_with_port + self._instances_with_no_port
        return sqlalchemy_query_jsonify_for_multiple_instances(
            query_function=dao.alarms.select_future_alarm,
            instances=instances, start_at=self._start, end_at=self._end, 
            group=True)

    @property
    def slow_sql_rca(self):
        topk_root_cause, topk_database = 10, 5
        query_template_recorder = {}
        query_type_distribution, root_cause_distribution = {}, {}
        query_templates = {'header': ['dbname', 'template', 'count'], 'rows': []}
        slow_sql_rcas = _get_result_from_rv(self._history_report, 'slow_sql_rca')
        for slow_sql_rca in slow_sql_rcas:
            if not slow_sql_rca:
                continue
            header = slow_sql_rca['query_type_distribution']['header']
            dbname_index = header.index('dbname')
            select_index = header.index('select')
            update_index = header.index('update')
            insert_index = header.index('insert')
            delete_index = header.index('delete')
            for row in slow_sql_rca['query_type_distribution']['rows']:
                dbname = row[dbname_index]
                if dbname not in query_type_distribution:
                    query_type_distribution[dbname] = {'select': [], 'update': [], 'delete': [], 'insert': [], 'sum': []}
                query_type_distribution[dbname]['select'].append(row[select_index])
                query_type_distribution[dbname]['update'].append(row[update_index])
                query_type_distribution[dbname]['delete'].append(row[delete_index])
                query_type_distribution[dbname]['insert'].append(row[insert_index])
                query_type_distribution[dbname]['sum'].append(row[select_index] + row[update_index] +
                                                              row[delete_index] + row[insert_index])
            for rca, count in slow_sql_rca['root_cause_distribution'].items():
                if rca not in root_cause_distribution:
                    root_cause_distribution[rca] = 0
                root_cause_distribution[rca] += count
            header = slow_sql_rca['query_templates']['header']
            dbname_index = header.index('dbname')
            template_index = header.index('template')
            count_index = header.index('count')
            for row in slow_sql_rca['query_templates']['rows']:
                dbname = row[dbname_index]
                template = row[template_index]
                count = row[count_index]
                if dbname not in query_template_recorder:
                    query_template_recorder[dbname] = {}
                if template not in query_template_recorder[dbname]:
                    query_template_recorder[dbname][template] = 0
                query_template_recorder[dbname][template] += count
        for dbname, template_detail in query_template_recorder.items():
            for template, count in template_detail.items():
                query_templates['rows'].append([dbname, template, count])
        query_templates['rows'].sort(key=lambda item: item[-1], reverse=True)
        root_cause_distribution = sorted(root_cause_distribution.items(), key=lambda item: item[1], reverse=True)
        query_type_distribution = sorted(query_type_distribution.items(), key=lambda item: item[1]['sum'], reverse=True)
        del(root_cause_distribution[topk_root_cause:])
        del(query_type_distribution[topk_database:])
        return {'timestamps': self._history_start_time, 'query_type_distribution': dict(query_type_distribution),
                'root_cause_distribution': dict(root_cause_distribution), 'query_templates': dict(query_templates)}

    @property
    def connection(self):
        instance_connection = {}
        connections = _get_result_from_rv(self._history_report, 'connection')
        for connection in connections:
            if not connection:
                continue
            for metric, value in connection.items():
                if metric not in instance_connection:
                    instance_connection[metric] = {'max': [], 'min': [], 'avg': [], 'the_95th': []}
                instance_connection[metric]['max'].append(value['max'])
                instance_connection[metric]['min'].append(value['min'])
                instance_connection[metric]['avg'].append(value['avg'])
                instance_connection[metric]['the_95th'].append(value['the_95th'])
        return {'timestamps': self._history_start_time, 'data': instance_connection}

    @property
    def dynamic_memory(self):
        instance_dynamic_memory = {}
        dynamic_memory = _get_result_from_rv(self._history_report, 'dynamic_memory')
        for instance_dynamic_memory_detail in dynamic_memory:
            for instance, memory_detail in instance_dynamic_memory_detail.items():
                statistic = memory_detail['statistic']
                if instance not in instance_dynamic_memory:
                    instance_dynamic_memory[instance] = {'max': [], 'min': [], 'avg': [], 'the_95th': []}
                instance_dynamic_memory[instance]['max'].append(statistic['max'])
                instance_dynamic_memory[instance]['min'].append(statistic['min'])
                instance_dynamic_memory[instance]['avg'].append(statistic['avg'])
                instance_dynamic_memory[instance]['the_95th'].append(statistic['the_95th'])
        return {'data': instance_dynamic_memory, 'timestamps': self._history_start_time}

    @property
    def risk(self):
        risks = {'header': ['instance', 'metric', 'event', 'occur time'], 'rows': []}
        return risks

    def __call__(self):
        return {'resource': self.resource,
                'performance': self.performance,
                'db_size': self.db_size,
                'table_size': self.table_size,
                'history_alarm': self.history_alarm,
                'future_alarm': self.future_alarm,
                'slow_sql_rca': self.slow_sql_rca,
                'connection': self.connection,
                'dynamic_memory': self.dynamic_memory,
                'risks': self.risk} 
