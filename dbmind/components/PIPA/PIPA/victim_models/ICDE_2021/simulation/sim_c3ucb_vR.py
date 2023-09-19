import datetime
import logging
import operator
import pprint
import time
from importlib import reload
import sys
import json
import numpy
from pandas import DataFrame

sys.path.append(json.load(open(sys.argv[1]))["experiments_root"] + "/victim_models/ICDE_2021")
import bandits.bandit_c3ucb_v2 as bandits
import bandits.bandit_helper_v2 as bandit_helper
import constants as constants
import database.sql_connection as sql_connection
import database.sql_helper_v2 as sql_helper
import shared.configs_v2 as configs
import shared.helper as helper
from bandits.experiment_report import ExpReport
from bandits.oracle_v2 import OracleV7 as Oracle
from bandits.query_v5 import Query
from psql import PostgreSQL as pg


# Simulation built on vQ to collect the super arm performance


class BaseSimulator:
    def __init__(self):
        # configuring the logger
        logging.basicConfig(
            filename=helper.get_experiment_folder_path(configs.experiment_id) + configs.experiment_id + '.log',
            filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
        logging.getLogger().setLevel(logging.INFO)

        # Get the query List
        self.queries = helper.get_queries_v2()
        self.connection = sql_connection.get_sql_connection()
        self.query_obj_store = {}
        reload(bandit_helper)


class Simulator(BaseSimulator):

    def __init__(self):
        super().__init__()
        self.pp = pprint.PrettyPrinter()
        reload(configs)
        self.results = []
        self.super_arm_scores = {}
        self.super_arm_counts = {}
        self.best_super_arm = set()
        logging.info("Logging configs...\n")
        helper.log_configs(logging, configs)
        logging.info("Logging constants...\n")
        helper.log_configs(logging, constants)
        logging.info("Starting MAB...\n")
        self.pg_client = pg.PGHypo(self.connection)

        # Get all the columns from the database
        self.all_columns, self.number_of_columns = sql_helper.get_all_columns(self.connection)
        self.context_size = self.number_of_columns * (
                1 + constants.CONTEXT_UNIQUENESS + constants.CONTEXT_INCLUDES) + constants.STATIC_CONTEXT_SIZE
        # Create oracle and the bandit
        configs.max_memory -= int(sql_helper.get_current_pds_size(self.connection))
        self.oracle = Oracle(configs.max_memory)
        self.c3ucb_bandit = bandits.C3UCB(self.context_size, configs.input_alpha, configs.input_lambda, self.oracle)
        self.arm_selection_count = {}
        self.chosen_arms_last_round = {}
        self.query_obj_list_current = []
        self.query_obj_list_past = []
        self.query_obj_list_new = []
        self.index_arms = {}
        self.chosen_arms = None
        self.context_vectors_v1 = None
        self.context_vectors_v2 = None
        self.context_vectors = []
        self.keys_last_round = None
        self.keys_this_round = None
        self.current_config_size = 0

    def run(self, sq):
        # Running the bandit for T rounds and gather the reward
        self.pg_client.delete_indexes()
        # init_cost = self.pg_client.get_queries_cost([sql,])[0] * 21
        init_cost = 0
        sql = []
        sql.append(
            "select part.p_size from part where part.p_partkey = 105685 and part.p_retailprice = 1296.28 and part.p_partkey = 191952;")
        sql.append(
            "select supplier.s_nationkey, lineitem.l_extendedprice, partsupp.ps_availqty from supplier join partsupp on partsupp.ps_suppkey=supplier.s_suppkey join lineitem on lineitem.l_partkey=partsupp.ps_partkey and lineitem.l_suppkey=partsupp.ps_suppkey where supplier.s_nationkey != 22;")
        sql.append("select orders.o_totalprice from orders where orders.o_totalprice = 243565.29;")
        sql.append(
            "select part.p_size, partsupp.ps_supplycost from part join partsupp on partsupp.ps_partkey=part.p_partkey where part.p_size >= 24 and part.p_size < 25.0 and part.p_partkey != 123941 and part.p_size < 47.0 order by part.p_size DESC;")
        sql.append(
            "select orders.o_totalprice, customer.c_nationkey from orders join customer on customer.c_custkey=orders.o_custkey where orders.o_custkey <= 64405;")
        self.queries = []
        for i in range(5):
            querys = helper.get_queries_v3(sql[i])
            for query in querys:
                logging.info(query)
                self.queries.append(query)
        # self.queries = helper.get_queries_v3(sql)
        if self.chosen_arms:
            sql_helper.bulk_create_indexes_v2(self.connection, self.chosen_arms)
        next_workload_shift = 0
        queries_start = configs.queries_start_list[next_workload_shift]
        queries_end = configs.queries_end_list[next_workload_shift]
        query_obj_additions = []
        total_time = 0.0

        for t in range((configs.rounds + configs.hyp_rounds)):
            logging.info(f"round: {t}")
            start_time_round = datetime.datetime.now()
            # At the start of the round we will read the applicable set for the current round. This is a workaround
            # used to demo the dynamic query flow. We read the queries from the start and move the window each round

            # check if workload shift is required
            if t - configs.hyp_rounds == configs.workload_shifts[next_workload_shift]:
                queries_start = configs.queries_start_list[next_workload_shift]
                queries_end = configs.queries_end_list[next_workload_shift]
                if len(configs.workload_shifts) > next_workload_shift + 1:
                    next_workload_shift += 1

            # New set of queries in this batch, required for query execution
            queries_current_batch = self.queries[queries_start:queries_end]

            # Adding new queries to the query store
            self.query_obj_list_current = []
            for n in range(len(queries_current_batch)):
                query = queries_current_batch[n]
                query_id = query['id']
                if query_id in self.query_obj_store:
                    query_obj_in_store = self.query_obj_store[query_id]
                    query_obj_in_store.frequency += 1
                    query_obj_in_store.last_seen = t
                    query_obj_in_store.query_string = query['query_string']
                    if query_obj_in_store.first_seen == -1:
                        query_obj_in_store.first_seen = t
                else:
                    query = Query(self.connection, query_id, query['query_string'], query['predicates'],
                                  query['payload'], t)
                    query.context = bandit_helper.get_query_context_v1(query, self.all_columns, self.number_of_columns)
                    self.query_obj_store[query_id] = query
                self.query_obj_list_current.append(self.query_obj_store[query_id])

            # This list contains all past queries, we don't include new queries seen for the first time.
            self.query_obj_list_past = []
            self.query_obj_list_new = []
            for key, obj in self.query_obj_store.items():
                if t - obj.last_seen <= constants.QUERY_MEMORY and 0 <= obj.first_seen < t:
                    self.query_obj_list_past.append(obj)
                elif t - obj.last_seen > constants.QUERY_MEMORY:
                    obj.first_seen = -1
                elif obj.first_seen == t:
                    self.query_obj_list_new.append(obj)

            # We don't want to reset in the first round, if there is new additions or removals we identify a
            # workload change
            if t > 0 and len(query_obj_additions) > 0:
                workload_change = len(query_obj_additions) / len(self.query_obj_list_past)
                self.c3ucb_bandit.workload_change_trigger(workload_change)

            # this rounds new will be the additions for the next round
            query_obj_additions = self.query_obj_list_new

            # Get the predicates for queries and Generate index arms for each query
            self.index_arms = {}
            for i in range(len(self.query_obj_list_past)):
                bandit_arms_tmp = bandit_helper.gen_arms_from_predicates_v2(self.connection, self.query_obj_list_past[i])
                for key, index_arm in bandit_arms_tmp.items():
                    if key not in self.index_arms:
                        index_arm.query_ids = set()
                        index_arm.query_ids_backup = set()
                        index_arm.clustered_index_time = 0
                        self.index_arms[key] = index_arm
                    index_arm.clustered_index_time += max(
                        self.query_obj_list_past[i].table_scan_times[index_arm.table_name]) if \
                        self.query_obj_list_past[i].table_scan_times[index_arm.table_name] else 0
                    self.index_arms[key].query_ids.add(index_arm.query_id)
                    self.index_arms[key].query_ids_backup.add(index_arm.query_id)
            # set the index arms at the bandit
            if t == configs.hyp_rounds and configs.hyp_rounds != 0:
                self.index_arms = {}
            index_arm_list = list(self.index_arms.values())
            logging.info(f"Generated {len(index_arm_list)} arms")
            self.c3ucb_bandit.set_arms(index_arm_list)

            # creating the context, here we pass all the columns in the database
            self.context_vectors_v1 = bandit_helper.get_name_encode_context_vectors_v2(self.index_arms, self.all_columns,
                                                                                  self.number_of_columns,
                                                                                  constants.CONTEXT_UNIQUENESS,
                                                                                  constants.CONTEXT_INCLUDES)
            self.context_vectors_v2 = bandit_helper.get_derived_value_context_vectors_v3(self.connection, self.index_arms, self.query_obj_list_past,
                                                                                    self.chosen_arms_last_round, not constants.CONTEXT_INCLUDES)
            self.context_vectors = []
            for i in range(len(self.context_vectors_v1)):
                self.context_vectors.append(
                    numpy.array(list(self.context_vectors_v2[i]) + list(self.context_vectors_v1[i]),
                                ndmin=2))
            # getting the super arm from the bandit
            chosen_arm_ids = self.c3ucb_bandit.select_arm_v2(self.context_vectors)
            logging.info("number of chosen_arm_ids = " + str(len(chosen_arm_ids)))
            if t >= configs.hyp_rounds and t - configs.hyp_rounds > constants.STOP_EXPLORATION_ROUND:
                chosen_arm_ids = list(self.best_super_arm)

            # get objects for the chosen set of arm ids
            self.chosen_arms = {}
            used_memory = 0
            if chosen_arm_ids:
                self.chosen_arms = {}
                for arm in chosen_arm_ids:
                    index_name = index_arm_list[arm].index_name
                    self.chosen_arms[index_name] = index_arm_list[arm]
                    used_memory = used_memory + index_arm_list[arm].memory
                    if index_name in self.arm_selection_count:
                        self.arm_selection_count[index_name] += 1
                    else:
                        self.arm_selection_count[index_name] = 1

            # clean everything at start of actual rounds
            logging.info("number of chosen_arms = " + str(len(self.chosen_arms)))
            if configs.hyp_rounds != 0 and t == configs.hyp_rounds:
                sql_helper.bulk_drop_index(self.connection, constants.SCHEMA_NAME, self.chosen_arms_last_round)
                self.chosen_arms_last_round = {}

            # finding the difference between last round and this round
            self.keys_last_round = set(self.chosen_arms_last_round.keys())
            self.keys_this_round = set(self.chosen_arms.keys())
            key_intersection = self.keys_last_round & self.keys_this_round
            key_additions = self.keys_this_round - key_intersection
            key_deletions = self.keys_last_round - key_intersection
            logging.info(f"Selected: {self.keys_this_round}")
            logging.debug(f"Added: {key_additions}")
            logging.debug(f"Removed: {key_deletions}")

            added_arms = {}
            deleted_arms = {}
            for key in key_additions:
                added_arms[key] = self.chosen_arms[key]
            for key in key_deletions:
                deleted_arms[key] = self.chosen_arms_last_round[key]

            start_time_create_query = datetime.datetime.now()
            if t < configs.hyp_rounds:
                time_taken, creation_cost_dict, arm_rewards = sql_helper.hyp_create_query_drop_v1(self.connection, constants.SCHEMA_NAME,
                                                                                                  self.chosen_arms, added_arms, deleted_arms,
                                                                                                  self.query_obj_list_current)
            else:
                time_taken, creation_cost_dict, arm_rewards = sql_helper.create_query_drop_v3(self.connection,
                                                                                              constants.SCHEMA_NAME,
                                                                                              self.chosen_arms, added_arms,
                                                                                              deleted_arms,
                                                                                              self.query_obj_list_current)
            end_time_create_query = datetime.datetime.now()
            creation_cost = sum(creation_cost_dict.values())
            if t == configs.hyp_rounds and configs.hyp_rounds != 0:
                # logging arm usage counts
                logging.info("\n\nIndex Usage Counts:\n" + self.pp.pformat(
                    sorted(self.arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)))
                self.arm_selection_count = {}

            self.c3ucb_bandit.update_v4(chosen_arm_ids, arm_rewards)
            super_arm_id = frozenset(chosen_arm_ids)
            if t >= configs.hyp_rounds:
                if super_arm_id in self.super_arm_scores:
                    self.super_arm_scores[super_arm_id] = self.super_arm_scores[super_arm_id] * self.super_arm_counts[super_arm_id] \
                                                     + time_taken
                    self.super_arm_counts[super_arm_id] += 1
                    self.super_arm_scores[super_arm_id] /= self.super_arm_counts[super_arm_id]
                else:
                    self.super_arm_counts[super_arm_id] = 1
                    self.super_arm_scores[super_arm_id] = time_taken

            # keeping track of queries that we saw last time
            self.chosen_arms_last_round = self.chosen_arms

            if t == (configs.rounds + configs.hyp_rounds - 1):
                sql_helper.bulk_drop_index(self.connection, constants.SCHEMA_NAME, self.chosen_arms)

            end_time_round = datetime.datetime.now()
            self.current_config_size = float(sql_helper.get_current_pds_size(self.connection))
            logging.info("Size taken by the config: " + str(self.current_config_size) + "MB")
            # Adding information to the results array
            if t >= configs.hyp_rounds:
                actual_round_number = t - configs.hyp_rounds
                recommendation_time = (end_time_round - start_time_round).total_seconds() - (
                            end_time_create_query - start_time_create_query).total_seconds()
                total_round_time = creation_cost + time_taken + recommendation_time
                self.results.append([actual_round_number, constants.MEASURE_BATCH_TIME, total_round_time])
                self.results.append([actual_round_number, constants.MEASURE_INDEX_CREATION_COST, creation_cost])
                self.results.append([actual_round_number, constants.MEASURE_QUERY_EXECUTION_COST, time_taken])
                self.results.append(
                    [actual_round_number, constants.MEASURE_INDEX_RECOMMENDATION_COST, recommendation_time])
                self.results.append([actual_round_number, constants.MEASURE_MEMORY_COST, self.current_config_size])
            else:
                total_round_time = (end_time_round - start_time_round).total_seconds() - (
                        end_time_create_query - start_time_create_query).total_seconds()
                self.results.append([t, constants.MEASURE_HYP_BATCH_TIME, total_round_time])
            total_time += total_round_time

            if t >= configs.hyp_rounds:
                self.best_super_arm = min(self.super_arm_scores, key=self.super_arm_scores.get)

            print(f"current total {t}: ", total_time)

        logging.info("Time taken by bandit for " + str(configs.rounds) + " rounds: " + str(total_time))
        logging.info("\n\nIndex Usage Counts:\n" + self.pp.pformat(
            sorted(self.arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)))
        # sql_helper.restart_sql_server()
        return self.results, total_time, self.chosen_arms, init_cost - time_taken


if __name__ == "__main__":
    # Running MAB
    exp_report_mab = ExpReport(configs.experiment_id, constants.COMPONENT_MAB, configs.reps, configs.rounds)
    for r in range(configs.reps):
        simulator = Simulator()
        sql = []
        sql.append("select part.p_size from part where part.p_partkey = 105685 and part.p_retailprice = 1296.28 and part.p_partkey = 191952;")
        sql.append("select supplier.s_nationkey, lineitem.l_extendedprice, partsupp.ps_availqty from supplier join partsupp on partsupp.ps_suppkey=supplier.s_suppkey join lineitem on lineitem.l_partkey=partsupp.ps_partkey and lineitem.l_suppkey=partsupp.ps_suppkey where supplier.s_nationkey != 22;")
        sql.append("select orders.o_totalprice from orders where orders.o_totalprice = 243565.29;")
        sql.append("select part.p_size, partsupp.ps_supplycost from part join partsupp on partsupp.ps_partkey=part.p_partkey where part.p_size >= 24 and part.p_size < 25.0 and part.p_partkey != 123941 and part.p_size < 47.0 order by part.p_size DESC;")
        sql.append("select orders.o_totalprice, customer.c_nationkey from orders join customer on customer.c_custkey=orders.o_custkey where orders.o_custkey <= 64405;")
        for i in range(1):
            sim_results, total_workload_time, chosen_arms, cost_reduction = simulator.run(sql[i])
            indexes = ""
            for index_name, chosen_arm in chosen_arms.items():
                strs = ""
                for col in chosen_arm.index_cols:
                    strs = strs + ', ' + chosen_arm.table_name + "." + col
                strs = strs[2::]
                indexes = indexes + ";(" + strs + ")"
            indexes = indexes[1:]
            logging.info("recommendation indexes = " + str(indexes))
            logging.info("cost_reduction = " + str(cost_reduction))
        temp = DataFrame(sim_results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                               constants.DF_COL_MEASURE_VALUE])
        temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
        temp[constants.DF_COL_REP] = r
        exp_report_mab.add_data_list(temp)

    # plot line graphs
    helper.plot_exp_report(configs.experiment_id, [exp_report_mab],
                           (constants.MEASURE_BATCH_TIME, constants.MEASURE_INDEX_CREATION_COST,
                            constants.MEASURE_QUERY_EXECUTION_COST, constants.MEASURE_INDEX_RECOMMENDATION_COST,
                            constants.MEASURE_MEMORY_COST))
