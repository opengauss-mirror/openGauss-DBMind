import itertools
import logging
import time

import numpy

import constants as constants
import database.sql_helper_v2 as sql_helper
from bandits.bandit_arm import BanditArm

bandit_arm_store = {}


def gen_arms_from_predicates_v2(connection, query_obj):
    """
    This method take predicates (a dictionary of lists) as input and creates the generate arms for all possible
    column combinations

    :param connection: SQL connection
    :param query_obj: Query object
    :return: list of bandit arms
    """
    bandit_arms = {}
    predicates = query_obj.predicates
    payloads = query_obj.payload
    query_id = query_obj.id
    tables = sql_helper.get_tables(connection)
    for table_name, table_predicates in predicates.items():
        table = tables[str.lower(table_name)]
        includes = []
        if table_name in payloads:
            includes = list(set(payloads[table_name]) - set(table_predicates))
        if table.table_row_count < constants.SMALL_TABLE_IGNORE or (
                query_obj.selectivity[table_name] > constants.TABLE_MIN_SELECTIVITY and len(includes) > 0):
            continue
        col_permutations = []
        if len(table_predicates) > 6:
            table_predicates = table_predicates[0:6]
        for j in range(1, (len(table_predicates) + 1)):
            col_permutations = col_permutations + list(itertools.permutations(table_predicates, j))
        for col_permutation in col_permutations:
            arm_id = BanditArm.get_arm_id(col_permutation, table_name)
            table_row_count = table.table_row_count
            arm_value = (1 - query_obj.selectivity[table_name]) * (
                        len(col_permutation) / len(table_predicates)) * table_row_count
            if arm_id in bandit_arm_store:
                bandit_arm = bandit_arm_store[arm_id]
                bandit_arm.query_id = query_id
                if query_id in bandit_arm.arm_value:
                    bandit_arm.arm_value[query_id] += arm_value
                    bandit_arm.arm_value[query_id] /= 2
                else:
                    bandit_arm.arm_value[query_id] = arm_value
            else:
                size = sql_helper.get_estimated_size_of_index_v1(connection,
                                                                 table_name, col_permutation)
                bandit_arm = BanditArm(col_permutation, table_name, size, table_row_count)
                bandit_arm.query_id = query_id
                if len(col_permutation) == len(table_predicates):
                    bandit_arm.cluster = table_name + '_' + str(query_id) + '_all'
                    if len(includes) == 0:
                        bandit_arm.is_include = 1
                bandit_arm.arm_value[query_id] = arm_value
                bandit_arm_store[arm_id] = bandit_arm
            if bandit_arm not in bandit_arms:
                bandit_arms[arm_id] = bandit_arm

    for table_name, table_payloads in payloads.items():
        if table_name not in predicates:
            table = tables[str.lower(table_name)]
            if table.table_row_count < constants.SMALL_TABLE_IGNORE:
                continue
            col_permutation = table_payloads
            arm_id = BanditArm.get_arm_id(col_permutation, table_name)
            table_row_count = table.table_row_count
            arm_value = 0.001 * table_row_count
            if arm_id in bandit_arm_store:
                bandit_arm = bandit_arm_store[arm_id]
                bandit_arm.query_id = query_id
                if query_id in bandit_arm.arm_value:
                    bandit_arm.arm_value[query_id] += arm_value
                    bandit_arm.arm_value[query_id] /= 2
                else:
                    bandit_arm.arm_value[query_id] = arm_value
            else:
                size = sql_helper.get_estimated_size_of_index_v1(connection,
                                                                 table_name, col_permutation)
                bandit_arm = BanditArm(col_permutation, table_name, size, table_row_count)
                bandit_arm.query_id = query_id
                bandit_arm.cluster = table_name + '_' + str(query_id) + '_all'
                bandit_arm.is_include = 1
                bandit_arm.arm_value[query_id] = arm_value
                bandit_arm_store[arm_id] = bandit_arm
            if bandit_arm not in bandit_arms:
                bandit_arms[arm_id] = bandit_arm

    if constants.INDEX_INCLUDES:
        for table_name, table_predicates in predicates.items():
            table = tables[str.lower(table_name)]
            if table.table_row_count < constants.SMALL_TABLE_IGNORE:
                continue
            includes = []
            if table_name in payloads:
                includes = sorted(list(set(payloads[table_name]) - set(table_predicates)))
            if includes:
                col_permutations = list(itertools.permutations(table_predicates, len(table_predicates)))
                for col_permutation in col_permutations:
                    arm_id_with_include = BanditArm.get_arm_id(col_permutation, table_name, includes)
                    table_row_count = table.table_row_count
                    arm_value = (1 - query_obj.selectivity[table_name]) * table_row_count
                    if arm_id_with_include not in bandit_arm_store:
                        size_with_includes = sql_helper.get_estimated_size_of_index_v1(connection,
                                                                                       table_name,
                                                                                       col_permutation + tuple(
                                                                                           includes))
                        bandit_arm = BanditArm(col_permutation, table_name, size_with_includes, table_row_count,
                                               includes)
                        bandit_arm.is_include = 1
                        bandit_arm.query_id = query_id
                        bandit_arm.cluster = table_name + '_' + str(query_id) + '_all'
                        bandit_arm.arm_value[query_id] = arm_value
                        bandit_arm_store[arm_id_with_include] = bandit_arm
                    else:
                        bandit_arm_store[arm_id_with_include].query_id = query_id
                        if query_id in bandit_arm_store[arm_id_with_include].arm_value:
                            bandit_arm_store[arm_id_with_include].arm_value[query_id] += arm_value
                            bandit_arm_store[arm_id_with_include].arm_value[query_id] /= 2
                        else:
                            bandit_arm_store[arm_id_with_include].arm_value[query_id] = arm_value
                    bandit_arms[arm_id_with_include] = bandit_arm_store[arm_id_with_include]
    return bandit_arms


def gen_arms_from_predicates_single(connection, query_obj):
    """
    This method take predicates (a dictionary of lists) as input and creates the generate arms for all possible
    column combinations

    :param connection: SQL connection
    :param query_obj: Query object
    :return: list of bandit arms
    """
    bandit_arms = {}
    predicates = query_obj.predicates
    query_id = query_obj.id
    tables = sql_helper.get_tables(connection)
    includes = []
    for table_name, table_predicates in predicates.items():
        table = tables[table_name]
        if table.table_row_count < 1000 or (
                query_obj.selectivity[table_name] > constants.TABLE_MIN_SELECTIVITY and len(includes) > 0):
            continue
        col_permutations = []
        if len(table_predicates) > 6:
            table_predicates = table_predicates[0:6]
        col_permutations = col_permutations + list(itertools.permutations(table_predicates, 1))
        for col_permutation in col_permutations:
            arm_id = BanditArm.get_arm_id(col_permutation, table_name)
            table_row_count = table.table_row_count
            arm_value = (1 - query_obj.selectivity[table_name]) * (
                        len(col_permutation) / len(table_predicates)) * table_row_count
            if arm_id in bandit_arm_store:
                bandit_arm = bandit_arm_store[arm_id]
                bandit_arm.query_id = query_id
                if query_id in bandit_arm.arm_value:
                    bandit_arm.arm_value[query_id] += arm_value
                    bandit_arm.arm_value[query_id] /= 2
                else:
                    bandit_arm.arm_value[query_id] = arm_value
            else:
                size = sql_helper.get_estimated_size_of_index_v1(connection, constants.SCHEMA_NAME,
                                                                 table_name, col_permutation)
                bandit_arm = BanditArm(col_permutation, table_name, size, table_row_count)
                bandit_arm.query_id = query_id
                if len(col_permutation) == len(table_predicates):
                    bandit_arm.cluster = table_name + '_' + str(query_id) + '_all'
                    if len(includes) == 0:
                        bandit_arm.is_include = 1
                bandit_arm.arm_value[query_id] = arm_value
                bandit_arm_store[arm_id] = bandit_arm
            if bandit_arm not in bandit_arms:
                bandit_arms[arm_id] = bandit_arm
                # print(arm_id)

    return bandit_arms
# ========================== Context Vectors ==========================


def get_predicate_position(arm, predicate, table_name):
    """
    Returns float between 0 and 1  if a arm includes a predicate for the the correct table

    :param arm: bandit arm
    :param predicate: given predicate
    :param table_name: table name
    :return: float [0, 1]
    """
    for i in range(len(arm.index_cols)):
        if table_name == arm.table_name and predicate == arm.index_cols[i]:
            return i
    return -1


def get_context_vector_v2(bandit_arm, all_columns, context_size, uniqueness=0, includes=False):
    """
    Return the context vector for a given arm, and set of predicates. Size of the context vector will depend on
    the arm and the set of predicates (for now on predicates)

    :param bandit_arm: bandit arm
    :param all_columns: predicate dict(list)
    :param context_size: size of the context vector
    :param uniqueness: how many columns in the index to consider when considering the context
    :param includes: add includes to the arm encode
    :return: a context vector
    """
    context_vectors = {}
    for j in range(uniqueness):
        context_vectors[j] = numpy.zeros((context_size, 1), dtype=float)
    left_over_context = numpy.zeros((context_size, 1), dtype=float)
    include_context = numpy.zeros((context_size, 1), dtype=float)

    if len(bandit_arm.name_encoded_context) > 0:
        context_vector = bandit_arm.name_encoded_context
    else:
        i = 0
        for table_name in all_columns:
            for k in range(len(all_columns[table_name])):
                column_position_in_arm = get_predicate_position(bandit_arm, all_columns[table_name][k], table_name)
                if column_position_in_arm >= 0:
                    if column_position_in_arm < uniqueness:
                        context_vectors[column_position_in_arm][i] = 1
                    else:
                        left_over_context[i] = 1 / (10 ** column_position_in_arm)
                elif all_columns[table_name][k] in bandit_arm.include_cols:
                    include_context[i] = 1
                i += 1

        full_list = []
        for j in range(uniqueness):
            full_list = full_list + list(context_vectors[j])
        full_list = full_list + list(left_over_context)
        if includes:
            full_list = full_list + list(include_context)
        context_vector = numpy.array(full_list, ndmin=2, dtype=float)
        bandit_arm.name_encoded_context = context_vector
    return context_vector


def get_name_encode_context_vectors_v2(bandit_arm_dict, all_columns, context_size, uniqueness=0, includes=False):
    """
    Return the context vectors for a given arms, and set of predicates.

    :param bandit_arm_dict: bandit arms
    :param all_columns: predicate dict(list)
    :param context_size: size of the context vector
    :param uniqueness: how many columns in the index to consider when considering the context
    :param includes: add includes to the arm encode
    :return: list of context vectors
    """
    context_vectors = []
    for key, bandit_arm in bandit_arm_dict.items():
        context_vector = get_context_vector_v2(bandit_arm, all_columns, context_size, uniqueness, includes)
        context_vectors.append(context_vector)

    return context_vectors


def get_derived_value_context_vectors_v3(connection, bandit_arm_dict, query_obj_list, chosen_arms_last_round,
                                         with_includes):
    """
    Similar to the v2, but it don't have the is_include part

    :param connection: SQL connection
    :param bandit_arm_dict: bandit arms
    :param query_obj_list: list of queries
    :param chosen_arms_last_round: Already created arms
    :param with_includes: have is include feature, note if includes are added to encode part we don't need it here.
    :return: list of context vectors
    """
    context_vectors = []
    database_size = sql_helper.get_database_size(connection)
    for key, bandit_arm in bandit_arm_dict.items():
        keys_last_round = set(chosen_arms_last_round.keys())
        if bandit_arm.index_name not in keys_last_round:
            index_size = bandit_arm.memory
        else:
            index_size = 0
        context_vector = numpy.array([
            bandit_arm.index_usage_last_batch,
            index_size/database_size,
            bandit_arm.is_include if with_includes else 0
        ], ndmin=2).transpose()
        context_vectors.append(context_vector)

    return context_vectors


def get_query_context_v1(query_object, all_columns, context_size):
    """
    Return the context vectors for a given query.

    :param query_object: query object
    :param all_columns: columns in database
    :param context_size: size of the context
    :return: list of context vectors
    """
    context_vector = numpy.zeros((context_size, 1), dtype=float)
    if query_object.context is not None:
        context_vector = query_object.context
    else:
        i = 0
        for table_name in all_columns:
            for k in range(len(all_columns[table_name])):
                context_vector[i] = 1 if table_name in query_object.predicates and all_columns[table_name][k] in \
                                         query_object.predicates[table_name] else 0
                i += 1
        query_object.context = context_vector
    return context_vector
