# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
import workload_generation.BartSqlGen.FSM.base_setting
import numpy as np
from .sample_data import MetaDataSupport
from treelib import Tree
import logging
import math
import sys
import json


np.set_printoptions(threshold=np.inf)

# support grammar key word
operator = ['=', '!=', '>', '<', '<=', '>=']
order_by_key = ['DESC', 'ASC']
# predicate_type = ['between', 'is null', 'is not null', 'in', 'is not in', 'exists', 'not exists', 'like', 'not like']
# conjunction = ['and', 'or']
# predicate_exist = ['exist', 'not exist']
predicate_in = ['in', 'not in']
conjunction = ['and']
aggregate = ['max', 'min', 'avg', 'sum']
# keyword = ['select', 'from', 'aggregate', 'where', 'group by', 'having', 'order by']
keyword = ['query', 'update', 'delete', 'insert', 'select', 'from', 'aggregate', 'where', 'having', 'order by',
           'subquery', 'into', 'set']  # group by是被迫的去掉了


class DataNode(object):
    # action_index 与 identifier不同, action_index是map里面的，identifire是tree里面的
    def __init__(self, action_index, datatype=None, key_type=None):
        self.action_index = action_index
        self.datatype = datatype
        self.key_type = key_type

class Common(object):
    step_reward = 0
    bug_reward = -100
    start_word = "@"
    terminal_word = " "     # sql terminal word
    sub_terminal_word = "#"     # subquery terminal word
    SEQ_LENGTH = 40

    def __init__(self,dbname,server_name = "postgresql"):
        self.dbname = dbname
        self.server_name = server_name
        self.task_name = "tpch_1gb"
        
        self.SampleData = MetaDataSupport(dbname)
        self.schema = self.SampleData.schema
        self.num_word_map_seperate = {}
        self.word_num_map, self.num_word_map, self.relation_tree = self._build_relation_env()

        self.relation_graph = workload_generation.BartSqlGen.FSM.base_setting.build_relation_graph(self.dbname, self.schema)

        self.action_space = self.observation_space = len(self.word_num_map)
        self.operator = [self.word_num_map[x] for x in operator]
        # self.predicate_exist = [self.word_num_map[x] for x in predicate_exist]
        self.predicate_in = [self.word_num_map[x] for x in predicate_in]
        # self.order_by_key = [self.word_num_map[x] for x in order_by_key]
        # self.predicate_type = [self.word_num_map[x] for x in predicate_type]
        self.conjunction = [self.word_num_map[x] for x in conjunction]
        # self.aggregate = [self.word_num_map[x] for x in aggregate]
        self.keyword = [self.word_num_map[x] for x in keyword]
        # self.integer = [self.word_num_map[x] for x in integer]
        # self.join = [self.word_num_map[x] for x in join]

        self.attributes = []

        table_node = self.relation_tree.children(self.relation_tree.root)
        self.tables = [field.identifier for field in table_node]
        for i in self.tables:
            if self.word_num_map["hypopg_list_indexes"] == i:
                self.tables.remove(i)
        for node in table_node:
            self.attributes += [field.identifier for field in self.relation_tree.children(node.identifier)]

    @staticmethod
    def add_map(series, word_num_map, num_word_map):
        count = len(word_num_map)
        for word in series:
            if word not in word_num_map.keys():
                word_num_map[word] = count
                num_word_map[count] = word
                count += 1

    def _build_relation_env(self):
        print("_build_env")

        sample_data = self.SampleData.get_data()

        tree = Tree()
        tree.create_node("root", 0, None, data=DataNode(0))

        word_num_map = dict()
        num_word_map = dict()

        word_num_map[self.start_word] = 0
        num_word_map[0] = self.start_word

        word_num_map[self.terminal_word] = 1
        num_word_map[1] = self.terminal_word

        word_num_map[self.sub_terminal_word] = 2
        num_word_map[2] = self.sub_terminal_word

        # 第一层 table_names
        count = 3
        for table_name in self.schema.keys():
            tree.create_node(table_name, count, parent=0, data=DataNode(count, datatype="table_name"))
            word_num_map[table_name] = count
            num_word_map[count] = table_name
            count += 1

        # 第二层 table的attributes
        for table_name in self.schema.keys():
            for field in self.schema[table_name]:
                attribute = '{0}.{1}'.format(table_name, field)
                tree.create_node(attribute, count, parent=word_num_map[table_name],
                                 data=DataNode(count))
                word_num_map[attribute] = count
                num_word_map[count] = attribute
                count += 1

        # 第三层 每个taoble的sample data
        for table_name in self.schema.keys():
            for field in self.schema[table_name]:
                for data in sample_data[table_name][field]:
                    if data in word_num_map.keys():
                        pass
                    else:
                        word_num_map[data] = len(num_word_map)
                        num_word_map[len(num_word_map)] = data
                    field_name = '{0}.{1}'.format(table_name, field)
                    tree.create_node(data, count, parent=word_num_map[field_name], data=DataNode(word_num_map[data]))
                    count += 1

        self.add_map(operator, word_num_map, num_word_map)
        self.add_map(order_by_key, word_num_map, num_word_map)
        # self.add_map(predicate_type, word_num_map, num_word_map)
        self.add_map(conjunction, word_num_map, num_word_map)
        self.add_map(aggregate, word_num_map, num_word_map)
        self.add_map(keyword, word_num_map, num_word_map)
        # self.add_map(predicate_exist, word_num_map, num_word_map)
        self.add_map(predicate_in, word_num_map, num_word_map)
        # self.add_map(integer, word_num_map,num_word_map)
        # self.add_map(join, word_num_map, num_word_map)

        print("_build_env done...")
        print("action/observation space:", len(num_word_map), len(word_num_map))
        print("relation tree size:", tree.size())
        # tree.show()
        return word_num_map, num_word_map, tree

    def activate_space(self, cur_space, keyword):   # 用keyword开启 cur_space 到 next_space 的门
        # 激活下一个space
        cur_space[self.word_num_map[keyword]] = 1

    def activate_terminal(self, cur_space):
        cur_space[self.word_num_map[self.terminal_word]] = 1

if __name__ == '__main__':
    a = MetaDataSupport('tpch_1gb')
    # a.load_statistics(refresh=True)
    a.get_data()



