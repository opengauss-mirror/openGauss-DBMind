# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
from src.plan_encoding.encoding_nodes import *


class TreeNode(object):

    def __init__(self, current_vec, parent, idx, level_id):
        self.item = current_vec
        self.idx = idx
        self.level_id = level_id
        self.parent = parent
        self.children = []

    def get_parent(self):
        return self.parent

    def get_item(self):
        return self.item

    def get_children(self):
        return self.children

    def add_child(self, child):
        self.children.append(child)

    def get_idx(self):
        return self.idx

    def __str__(self):
        return 'level_id: ' + self.level_id + '; idx: ' + self.idx


def recover_tree(vecs, parent, start_idx):
    if len(vecs) == 0:
        return vecs, start_idx
    if vecs[0] == None:
        return vecs[1:], start_idx + 1
    node = TreeNode(vecs[0], parent, start_idx, -1)
    while True:
        vecs, start_idx = recover_tree(vecs[1:], node, start_idx + 1)
        parent.add_child(node)
        if len(vecs) == 0:
            return vecs, start_idx
        if vecs[0] == None:
            return vecs[1:], start_idx + 1
        node = TreeNode(vecs[0], parent, start_idx, -1)


def dfs_tree_to_level(root, level_id, nodes_by_level):
    root.level_id = level_id
    if len(nodes_by_level) <= level_id:
        nodes_by_level.append([])
    nodes_by_level[level_id].append(root)
    root.idx = len(nodes_by_level[level_id])
    for c in root.get_children():
        dfs_tree_to_level(c, level_id + 1, nodes_by_level)


def encode_plans_job(plans, parameters):
    plans_embed = []
    for plan in plans:
        plan_embed = encode_plan_job(plan, parameters)
        plans_embed.append(plan_embed)
    return np.array(plans_embed)


def encode_plan_job(plan, parameters):
    plan_encoding_dfs = []
    for node in plan:
        operator_vec, extra_info_vec, condition_vec, has_condition = encode_node_job(
            node, parameters)
        for i, c_vec in enumerate(condition_vec):
            total_vec = np.concatenate(
                (operator_vec, extra_info_vec, c_vec, np.array([has_condition
                                                                ])),
                axis=0)
            aa = parameters.physic_op_total_num + parameters.column_total_num + parameters.condition_op_dim + 1
            plan_encoding_dfs.append(
                np.pad(total_vec, (0, 456 - aa), 'constant'))
    return plan_encoding_dfs
