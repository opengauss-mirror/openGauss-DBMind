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

from dbmind.common.algorithm import basic
from dbmind.common.types.root_cause import RootCause
from dbmind.common.utils.checking import prepare_ip


class SlowQuery:
    def __init__(self, query, db_host=None, db_port=None, db_name=None, schema_name='public', start_time=0,
                 finish_time=0, fetch_rate=0, cpu_time=0, data_io_time=0, track_parameter=True, advise=None,
                 template_id='', sort_count=0, sort_mem_used=0, sort_spill_count=0, hash_count=0, plan_time=0,
                 hash_mem_used=0, hash_spill_count=0, lock_wait_time=0, lwlock_wait_time=0, parse_time=0,
                 n_returned_rows=0, n_tuples_returned=0, n_tuples_fetched=0, n_tuples_inserted=0, db_time=0,
                 n_tuples_updated=0, n_tuples_deleted=0, user_name=None, query_plan=None, debug_query_id='',
                 n_soft_parse=0, n_hard_parse=0, n_blocks_hit=1, n_blocks_fetched=1, n_calls=1, **kwargs):
        self.db_host = db_host
        self.db_port = db_port
        self.user_name = user_name
        self.schema_name = schema_name
        self.db_name = db_name
        self.tables_name = None
        self.sort_condition = None
        self.query = query
        self.advise = advise
        self.track_parameter = track_parameter
        self.query_plan = query_plan
        self.start_time = start_time  # unit: microsecond
        self.finish_time = finish_time  # unit: microsecond
        self.duration_time = self.finish_time - self.start_time
        self.hit_rate = round(n_blocks_hit / n_blocks_fetched, 4) if n_blocks_fetched else 1
        self.fetch_rate = fetch_rate
        self.cpu_time = cpu_time
        self.data_io_time = data_io_time
        self.plan_time = plan_time
        self.parse_time = parse_time
        self.db_time = db_time
        self.template_id = template_id
        self.debug_query_id = debug_query_id
        self.sort_count = sort_count
        self.sort_mem_used = sort_mem_used
        self.n_calls = n_calls
        self.sort_spill_count = sort_spill_count / n_calls if n_calls else 0
        self.hash_count = hash_count
        self.hash_mem_used = hash_mem_used
        self.hash_spill_count = hash_spill_count / n_calls if n_calls else 0
        self.lwlock_wait_time = lwlock_wait_time
        self.lock_wait_time = lock_wait_time
        self.n_returned_rows = n_returned_rows
        self.n_tuples_returned = n_tuples_returned
        self.n_tuples_fetched = n_tuples_fetched
        self.n_tuples_inserted = n_tuples_inserted
        self.n_tuples_updated = n_tuples_updated
        self.n_tuples_deleted = n_tuples_deleted
        self.n_soft_parse = n_soft_parse
        self.n_hard_parse = n_hard_parse
        self.alarm_cause = list()

        self._discardable = False

        self.kwargs = kwargs

    def add_cause(self, root_cause: RootCause):
        self.alarm_cause.append(root_cause)
        return self

    @property
    def root_causes(self):
        lines = list()
        index = 1
        for c in self.alarm_cause:
            lines.append(
                '%d. %s: (%.2f) %s' % (index, c.title, c.probability, c.detail)
            )
            index += 1
        return '\n'.join(lines)

    @property
    def suggestions(self):
        lines = list()
        index = 1
        for c in self.alarm_cause:
            lines.append(
                '%d. %s' % (index, c.suggestion if c.suggestion else 'No suggestions.')
            )
            index += 1
        return '\n'.join(lines)

    @property
    def alarm_content(self):
        return 'Found slow query'

    @property
    def instance(self):
        return f"{prepare_ip(self.db_host)}:{self.db_port}"

    def mark_replicated(self):
        """If the same slow query has been diagnosed, we
        can discard this instance to avoid replicated storage.
        """
        self._discardable = True

    @property
    def replicated(self):
        return self._discardable

    def hash_query(self, use_root_cause=True):
        """Generate a hashcode for a specific slow query.
        This function does not allow to contrast two SlowQuery objects. It only
        offers a way to check whether two queries are same.
        :return: a pair of hashcode, to try to avoid hash conflicts.
        """
        query = self.query
        db_name = self.db_name.upper()
        schema_name = self.schema_name.upper()
        materials = [query, db_name, schema_name]
        if use_root_cause:
            root_cause_titles = (cause.title for cause in self.alarm_cause)
            root_causes = ' '.join(root_cause_titles)
            materials.append(root_causes)
        material_text = ''.join(materials)
        h1 = basic.dek_hash(material_text)
        h2 = basic.djb_hash(material_text)
        return h1, h2
