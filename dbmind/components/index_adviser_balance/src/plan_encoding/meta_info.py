import json
import math
import pickle


def load_numeric_min_max(path):
    with open(path, 'r') as f:
        min_max_column = json.loads(f.read())
    return min_max_column


def determine_prefix(column):
    relation_name = column.split('.')[0]
    column_name = column.split('.')[1]
    if relation_name == 'aka_title':
        if column_name == 'title':
            return 'title_'
        else:
            print(column)
            raise
    elif relation_name == 'char_name':
        if column_name == 'name':
            return 'name_'
        elif column_name == 'name_pcode_nf':
            return 'nf_'
        elif column_name == 'surname_pcode':
            return 'surname_'
        else:
            print(column)
            raise
    elif relation_name == 'movie_info_idx':
        if column_name == 'info':
            return 'info_'
        else:
            print(column)
            raise
    elif relation_name == 'title':
        if column_name == 'title':
            return 'title_'
        else:
            print(column)
            raise
    elif relation_name == 'role_type':
        if column_name == 'role':
            return 'role_'
        else:
            print(column)
            raise
    elif relation_name == 'movie_companies':
        if column_name == 'note':
            return 'note_'
        else:
            print(column)
            raise
    elif relation_name == 'info_type':
        if column_name == 'info':
            return 'info_'
        else:
            print(column)
            raise
    elif relation_name == 'company_type':
        if column_name == 'kind':
            return ''
        else:
            print(column)
            raise
    elif relation_name == 'company_name':
        if column_name == 'name':
            return 'cn_name_'
        elif column_name == 'country_code':
            return 'country_'
        else:
            print(column)
            raise
    elif relation_name == 'keyword':
        if column_name == 'keyword':
            return 'keyword_'
        else:
            print(column)
            raise

    elif relation_name == 'movie_info':
        if column_name == 'info':
            return ''
        elif column_name == 'note':
            return 'note_'
        else:
            print(column)
            raise
    elif relation_name == 'name':
        if column_name == 'gender':
            return 'gender_'
        elif column_name == 'name':
            return 'name_'
        elif column_name == 'name_pcode_cf':
            return 'cf_'
        elif column_name == 'name_pcode_nf':
            return 'nf_'
        elif column_name == 'surname_pcode':
            return 'surname_'
        else:
            print(column)
            raise
    elif relation_name == 'aka_name':
        if column_name == 'name':
            return 'name_'
        elif column_name == 'name_pcode_cf':
            return 'cf_'
        elif column_name == 'name_pcode_nf':
            return 'nf_'
        elif column_name == 'surname_pcode':
            return 'surname_'
        else:
            print(column)
            raise
    elif relation_name == 'link_type':
        if column_name == 'link':
            return 'link_'
        else:
            print(column)
            raise
    elif relation_name == 'person_info':
        if column_name == 'note':
            return 'note_'
        else:
            print(column)
            raise
    elif relation_name == 'cast_info':
        if column_name == 'note':
            return 'note_'
        else:
            print(column)
            raise
    elif relation_name == 'comp_cast_type':
        if column_name == 'kind':
            return 'kind_'
        else:
            print(column)
            raise
    elif relation_name == 'kind_type':
        if column_name == 'kind':
            return 'kind_'
        else:
            print(column)
            raise
    else:
        print(column)
        raise


def obtain_upper_bound_query_size(path):
    plan_node_max_num = 0
    condition_max_num = 0
    cost_label_max = 0.0
    cost_label_min = 9999999999.0
    card_label_max = 0.0
    card_label_min = 9999999999.0
    plans = []
    with open(path, 'r') as f:
        for plan in f.readlines():
            plan = json.loads(plan)
            plans.append(plan)
            cost = plan['cost']
            cardinality = plan['cardinality']
            if cost > cost_label_max:
                cost_label_max = cost
            elif cost < cost_label_min:
                cost_label_min = cost
            if cardinality > card_label_max:
                card_label_max = cardinality
            elif cardinality < card_label_min:
                card_label_min = cardinality
            sequence = plan['seq']
            plan_node_num = len(sequence)
            if plan_node_num > plan_node_max_num:
                plan_node_max_num = plan_node_num
            for node in sequence:
                if node == None:
                    continue
                if 'condition_filter' in node:
                    condition_num = len(node['condition_filter'])
                    if condition_num > condition_max_num:
                        condition_max_num = condition_num
                if 'condition_index' in node:
                    condition_num = len(node['condition_index'])
                    if condition_num > condition_max_num:
                        condition_max_num = condition_num
    cost_label_min, cost_label_max = math.log(cost_label_min), math.log(
        cost_label_max)
    card_label_min, card_label_max = math.log(card_label_min), math.log(
        card_label_max)
    print(plan_node_max_num, condition_max_num)
    print(cost_label_min, cost_label_max)
    print(card_label_min, card_label_max)
    return plan_node_max_num, condition_max_num, cost_label_min, cost_label_max, card_label_min, card_label_max


def prepare_dataset():

    column2pos = dict()

    tables = [
        'customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp',
        'region', 'supplier'
    ]



    column2pos['customer'] = {
        'c_mktsegment': 0,
        'c_nationkey': 1,
        'c_custkey': 2
    }
    column2pos['lineitem'] = {
        'l_shipinstruct': 0,
        'l_partkey': 1,
        'l_commitdate': 2,
        'l_suppkey': 3,
        'l_returnflag': 4,
        'l_orderkey': 5,
        'l_shipdate': 6,
        'l_quantity': 7,
        'l_shipmode': 8,
        'l_receiptdate': 9
    }
    column2pos['nation'] = {'n_nationkey': 0, 'n_name': 1, 'n_regionkey': 2}
    column2pos['orders'] = {
        'o_comment': 0,
        'o_orderkey': 1,
        'o_orderstatus': 2,
        'o_shippriority': 3,
        'o_custkey': 4,
        'o_orderpriority': 5,
        'o_orderdate': 6
    }
    column2pos['part'] = {
        'p_size': 0,
        'p_partkey': 1,
        'p_container': 2,
        'p_brand': 3,
        'p_name': 4,
        'p_type': 5
    }
    column2pos['partsupp'] = {'ps_partkey': 0, 'ps_suppkey': 1}
    column2pos['region'] = {'r_name': 0, 'r_regionkey': 1}
    column2pos['supplier'] = {
        's_suppkey': 0,
        's_nationkey': 1,
        's_name': 2,
        's_comment': 3
    }

    physic_ops_id = {
        'Materialize': 1,
        'Sort': 2,
        'Hash': 3,
        'Merge Join': 4,
        'Bitmap Index Scan': 5,
        'Index Only Scan': 6,
        'BitmapAnd': 7,
        'Nested Loop': 8,
        'Aggregate': 9,
        'Result': 10,
        'Hash Join': 11,
        'Seq Scan': 12,
        'Bitmap Heap Scan': 13,
        'Index Scan': 14,
        'BitmapOr': 15,
        'Memoize': 16,
        'Merge Append': 17,
        'Group': 18,
        'CTE Scan': 19
    }

    compare_ops_id = {
        '=': 1,
        '>': 2,
        '<': 3,
        '!=': 4,
        '~~': 5,
        '!~~': 6,
        '!Null': 7,
        '>=': 8,
        '<=': 9
    }
    bool_ops_id = {'AND': 1, 'OR': 2}
    tables_id = {}
    columns_id = {}
    table_id = 1
    column_id = 1
    for table_name in tables:
        tables_id[table_name] = table_id
        table_id += 1
        for column in column2pos[table_name]:
            columns_id[table_name + '.' + column] = column_id
            column_id += 1

    columnTypeisNum = [
        'customer.c_custkey', 'customer.c_nationkey', 'lineitem.l_partkey',
        'lineitem.l_orderkey', 'lineitem.l_linenumber', 'lineitem.l_suppkey',
        'nation.n_nationkey', 'nation.n_regionkey', 'orders.o_orderkey',
        'orders.o_shippriority', 'orders.o_custkey', 'part.p_size',
        'part.p_partkey', 'partsupp.ps_partkey', 'partsupp.ps_suppkey',
        'region.r_regionkey', 'supplier.s_suppkey', 'supplier.s_nationkey'
    ]
    box_lines = pickle.load(open('./experiment_utils/box_line.pickle', 'rb'))

    for table, columns in box_lines.items():
        for column, box_line in columns.items():
            column_name = table + '.' + column
            if column_name in columnTypeisNum:
                for i in range(len(box_line)):
                    box_line[i] = float(box_line[i])
            else:
                for i in range(len(box_line)):
                    box_line[i] = str(box_line[i])

    return column2pos, tables_id, columns_id, physic_ops_id, compare_ops_id, bool_ops_id, tables, columnTypeisNum, box_lines