from src.feature_extraction.node_features import *


def plan2seq_dfs(root, alias2table, sequence):

    if root['Node Type'] not in [
            'Append', 'Subquery Scan', 'Unique', 'SetOp', 'WindowAgg'
    ]:
        node, join_condition = extract_info_from_node(root, alias2table)
        if node != None:
            sequence.append(node)

    if 'Plans' in root:
        for plan in root['Plans']:
            plan2seq_dfs(plan, alias2table, sequence)
