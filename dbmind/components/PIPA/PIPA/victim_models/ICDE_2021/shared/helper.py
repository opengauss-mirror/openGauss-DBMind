# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Portions Copyright (c) 2025 Huawei Technologies Co.,Ltd.
# This file has been modified to adapt to openGauss.
import configparser
import json
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from pandas import DataFrame

import constants


def plot_histogram(statistics_dict, title, experiment_id):
    """
    Simple plot function to plot the average reward, and a line to show the best possible reward

    :param statistics_dict: list of statistic histograms
    :param title: title of the plot
    :param experiment_id: id of the current experiment
    """
    for statistic_name, statistic_histogram in statistics_dict.items():
        plt.plot(statistic_histogram, label=statistic_name)
    plt.title(title)
    plt.xlabel('Rounds')
    plt.legend()
    plt.savefig(get_experiment_folder_path(experiment_id) + title + '.png')
    plt.show()


def plot_histogram_v2(statistics_dict, window_size,  title, experiment_id):
    """
    Simple plot function to plot the average reward, and a line to show the best possible reward

    :param statistics_dict: list of statistic histograms
    :param window_size: size of the moving window
    :param title: title of the plot
    :param experiment_id: id of the current experiment
    """
    crop_amount = int(np.ceil(window_size/2))
    for statistic_name, statistic_histogram in statistics_dict.items():
        plt.plot(statistic_histogram[crop_amount:-crop_amount], label=statistic_name)
    plt.title(title)
    plt.xlabel('Rounds')
    plt.legend()
    plt.savefig(get_experiment_folder_path(experiment_id) + title + '.png')
    plt.show()


def get_experiment_folder_path(experiment_id):
    """
    Get the folder location of the experiment
    :param experiment_id: name of the experiment
    :return: file path as string
    """
    experiment_folder_path = os.getcwd() + '/result/ID_' + json.load(open(sys.argv[1]))["experiments_id"] + '/' + experiment_id + '/'
    if not os.path.exists(experiment_folder_path):
        os.makedirs(experiment_folder_path)
    return experiment_folder_path

def get_experiment_folder_path_v2(experiment_id):
    """
    Get the folder location of the experiment
    :param experiment_id: name of the experiment
    :return: file path as string
    """
    experiment_folder_path = os.getcwd() + '/result/ID_' + json.load(open(sys.argv[1]))["experiments_id"] + '/after_poison_' + experiment_id + '/ '
    if not os.path.exists(experiment_folder_path):
        os.makedirs(experiment_folder_path)
    return experiment_folder_path

def get_experiment_folder_path_v3(experiment_id, number):
    """
    Get the folder location of the experiment
    :param experiment_id: name of the experiment
    :return: file path as string
    """
    experiment_folder_path = os.getcwd() + '/result/ID_' + json.load(open(sys.argv[1]))["experiments_id"] + '/porbing'+ str(number) +'_' + experiment_id + '/ '
    if not os.path.exists(experiment_folder_path):
        os.makedirs(experiment_folder_path)
    return experiment_folder_path

def get_workload_folder_path(experiment_id):
    """
    Get the folder location of the experiment
    :param experiment_id: name of the experiment
    :return: file path as string
    """
    experiment_folder_path = constants.ROOT_DIR + constants.WORKLOADS_FOLDER + '/' + experiment_id + '/'
    if not os.path.exists(experiment_folder_path):
        os.makedirs(experiment_folder_path)
    return experiment_folder_path

def get_workload_folder_path_v2(experiment_id):
    """
    Get the folder location of the experiment
    :param experiment_id: name of the experiment
    :return: file path as string
    """
    experiment_folder_path = constants.ROOT_DIR + constants.WORKLOADS_FOLDER + '/after_poison_' + experiment_id + '/'
    if not os.path.exists(experiment_folder_path):
        os.makedirs(experiment_folder_path)
    return experiment_folder_path

def plot_histogram_avg(statistic_dict, title, experiment_id):
    """
    Simple plot function to plot the average reward, and a line to show the best possible reward
    :param statistic_dict: list of statistic histograms
    :param title: title of the plot
    :param experiment_id: id of the current experiment
    """
    for statistic_name, statistic_list in statistic_dict.items():
        for i in range(1, len(statistic_list)):
            statistic_list[i] = statistic_list[i - 1] + statistic_list[i]
            statistic_list[i - 1] = statistic_list[i - 1] / i
        statistic_list[len(statistic_list) - 1] = statistic_list[len(statistic_list) - 1] / len(statistic_list)

    plot_histogram(statistic_dict, title, experiment_id)


def plot_moving_average(statistic_dict, window_size, title, experiment_id):
    """
    Simple plot function to plot the moving average of a histograms
    :param statistic_dict: list of statistic histograms
    :param window_size: size of the moving window
    :param title: title of the plot
    :param experiment_id: id of the current experiment
    """
    statistic_avg_dict = {}
    for statistic_name, statistic_list in statistic_dict.items():
        avg_mask = np.ones(window_size) / window_size
        statistic_list_avg = np.convolve(statistic_list, avg_mask, 'same')
        statistic_avg_dict[statistic_name] = statistic_list_avg

    plot_histogram_v2(statistic_avg_dict, window_size, title, experiment_id)


def get_queries_v2():
    """
    Read all the queries in the queries pointed by the QUERY_DICT_FILE constant
    :return: list of queries
    """
    # Reading the configuration for given experiment ID
    exp_config = configparser.ConfigParser()
    exp_config.read(constants.ROOT_DIR + constants.EXPERIMENT_CONFIG)

    # experiment id for the current run
    experiment_id = exp_config['general']['run_experiment']
    workload_file = str(exp_config[experiment_id]['workload_file'])

    queries = []
    with open(constants.ROOT_DIR + workload_file) as f:
        line = f.readline()
        while line:
            queries.append(json.loads(line))
            line = f.readline()
    return queries

def get_queries_v3(sql, num):
    queries = []
    LINEITEM = ['LINEITEM.L_ORDERKEY', 'LINEITEM.L_PARTKEY', 'LINEITEM.L_SUPPKEY', 'LINEITEM.L_LINENUMBER',
                'LINEITEM.L_QUANTITY', 'LINEITEM.L_EXTENDEDPRICE', 'LINEITEM.L_DISCOUNT', 'LINEITEM.L_TAX',
                'LINEITEM.L_SHIPDATE', 'LINEITEM.L_COMMITDATE', 'LINEITEM.L_RECEIPTDATE', 'LINEITEM.L_SHIPINSTRUCT',
                'LINEITEM.L_SHIPMODE', 'LINEITEM.L_COMMENT', 'LINEITEM.L_RETURNFLAG', 'LINEITEM.L_LINESTATUS']
    CUSTOMER = ['CUSTOMER.C_CUSTKEY', 'CUSTOMER.C_NATIONKEY', 'CUSTOMER.C_ACCTBAL', 'CUSTOMER.C_NAME',
                'CUSTOMER.C_ADDRESS', 'CUSTOMER.C_PHONE', 'CUSTOMER.C_MKTSEGMENT', 'CUSTOMER.C_COMMENT']
    NATION = ['NATION.N_NATIONKEY', 'NATION.N_REGIONKEY', 'NATION.N_NAME', 'NATION.N_COMMENT']
    ORDERS = ['ORDERS.O_ORDERKEY', 'ORDERS.O_CUSTKEY', 'ORDERS.O_TOTALPRICE', 'ORDERS.O_ORDERDATE',
              'ORDERS.O_SHIPPRIORITY', 'ORDERS.O_ORDERSTATUS', 'ORDERS.O_ORDERPRIORITY', 'ORDERS.O_CLERK',
              'ORDERS.O_COMMENT']
    PART = ['PART.P_PARTKEY', 'PART.P_SIZE', 'PART.P_RETAILPRICE', 'PART.P_COMMENT', 'PART.P_CONTAINER', 'PART.P_NAME',
            'PART.P_MFGR', 'PART.P_BRAND', 'PART.P_TYPE']
    PARTSUPP = ['PARTSUPP.PS_PARTKEY', 'PARTSUPP.PS_SUPPKEY', 'PARTSUPP.PS_AVAILQTY', 'PARTSUPP.PS_SUPPLYCOST',
                'PARTSUPP.PS_COMMENT']
    REGION = ['REGION.R_REGIONKEY', 'REGION.R_NAME', 'REGION.R_COMMENT']
    SUPPLIER = ['SUPPLIER.S_SUPPKEY', 'SUPPLIER.S_NATIONKEY', 'SUPPLIER.S_ACCTBAL', 'SUPPLIER.S_NAME',
                'SUPPLIER.S_ADDRESS', 'SUPPLIER.S_PHONE', 'SUPPLIER.S_COMMENT']
    for i in range(2):
        for j in range(18):
            predicates = "{"
            payload = "{"
            for k in range(len(sql[j].split("select ")) - 1):
                select = sql[j].split("select")[k + 1].split(" from")[0]
                item1 = []
                for key in LINEITEM:
                    if str.lower(key) in select and key not in item1:
                        item1.append(key)
                item2 = []
                for key in CUSTOMER:
                    if str.lower(key) in select and key not in item2:
                        item2.append(key)
                item3 = []
                for key in NATION:
                    if str.lower(key) in select and key not in item3:
                        item3.append(key)
                item4 = []
                for key in ORDERS:
                    if str.lower(key) in select and key not in item4:
                        item4.append(key)
                item5 = []
                for key in PART:
                    if str.lower(key) in select and key not in item5:
                        item5.append(key)
                item6 = []
                for key in PARTSUPP:
                    if str.lower(key) in select and key not in item6:
                        item6.append(key)
                item7 = []
                for key in REGION:
                    if str.lower(key) in select and key not in item7:
                        item7.append(key)
                item8 = []
                for key in SUPPLIER:
                    if str.lower(key) in select and key not in item8:
                        item8.append(key)
            if len(item1) > 0:
                payload = payload + "\"LINEITEM\":["
                attrs = ""
                for key in item1:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\""
                attrs = attrs[2:]
                payload = payload + attrs + "], "
            if len(item2) > 0:
                payload = payload + "\"CUSTOMER\":["
                attrs = ""
                for key in item2:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\""
                attrs = attrs[2:]
                payload = payload + attrs + "], "
            if len(item3) > 0:
                payload = payload + "\"NATION\":["
                attrs = ""
                for key in item3:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\""
                attrs = attrs[2:]
                payload = payload + attrs + "], "
            if len(item4) > 0:
                payload = payload + "\"ORDERS\":["
                attrs = ""
                for key in item4:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\""
                attrs = attrs[2:]
                payload = payload + attrs + "], "
            if len(item5) > 0:
                payload = payload + "\"PART\":["
                attrs = ""
                for key in item5:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\""
                attrs = attrs[2:]
                payload = payload + attrs + "], "
            if len(item6) > 0:
                payload = payload + "\"PARTSUPP\":["
                attrs = ""
                for key in item6:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\""
                attrs = attrs[2:]
                payload = payload + attrs + "], "
            if len(item7) > 0:
                payload = payload + "\"REGION\":["
                attrs = ""
                for key in item7:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\""
                attrs = attrs[2:]
                payload = payload + attrs + "], "
            if len(item8) > 0:
                payload = payload + "\"SUPPLIER\":["
                attrs = ""
                for key in item8:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\""
                attrs = attrs[2:]
                payload = payload + attrs + "], "
            payload = payload[0:len(payload) - 2]
            payload += "}"
            item1 = []
            item2 = []
            item3 = []
            item4 = []
            item5 = []
            item6 = []
            item7 = []
            item8 = []
            for k in range(len(sql[j].split(" where ")) - 1):
                where = sql[j].split(" where ")[k + 1].split("order by")[0].split("group by")[0]
                for key in LINEITEM:
                    if str.lower(key) in where and key not in item1:
                        item1.append(key)
                for key in CUSTOMER:
                    if str.lower(key) in where and key not in item2:
                        item2.append(key)
                for key in NATION:
                    if str.lower(key) in where and key not in item3:
                        item3.append(key)
                for key in ORDERS:
                    if str.lower(key) in where and key not in item4:
                        item4.append(key)
                for key in PART:
                    if str.lower(key) in where and key not in item5:
                        item5.append(key)
                for key in PARTSUPP:
                    if str.lower(key) in where and key not in item6:
                        item6.append(key)
                for key in REGION:
                    if str.lower(key) in where and key not in item7:
                        item7.append(key)
                for key in SUPPLIER:
                    if str.lower(key) in where and key not in item8:
                        item8.append(key)
            for k in range(len(sql[j].split(" join ")) - 1):
                join = sql[j].split(" join")[k + 1].split("order by")[0].split("group by")[0].split("where")[0]
                for key in LINEITEM:
                    if str.lower(key) in join and key not in item1:
                        item1.append(key)
                for key in CUSTOMER:
                    if str.lower(key) in join and key not in item2:
                        item2.append(key)
                for key in NATION:
                    if str.lower(key) in join and key not in item3:
                        item3.append(key)
                for key in ORDERS:
                    if str.lower(key) in join and key not in item4:
                        item4.append(key)
                for key in PART:
                    if str.lower(key) in join and key not in item5:
                        item5.append(key)
                for key in PARTSUPP:
                    if str.lower(key) in join and key not in item6:
                        item6.append(key)
                for key in REGION:
                    if str.lower(key) in join and key not in item7:
                        item7.append(key)
                for key in SUPPLIER:
                    if str.lower(key) in join and key not in item8:
                        item8.append(key)
            if len(item1) > 0:
                predicates = predicates + "\"LINEITEM\":{"
                attrs = ""
                for key in item1:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\": \"r\""
                attrs = attrs[2:]
                predicates = predicates + attrs + "}, "
            if len(item2) > 0:
                predicates = predicates + "\"CUSTOMER\":{"
                attrs = ""
                for key in item2:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\": \"r\""
                attrs = attrs[2:]
                predicates = predicates + attrs + "}, "
            if len(item3) > 0:
                predicates = predicates + "\"NATION\":{"
                attrs = ""
                for key in item3:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\": \"r\""
                attrs = attrs[2:]
                predicates = predicates + attrs + "}, "
            if len(item4) > 0:
                predicates = predicates + "\"ORDERS\":{"
                attrs = ""
                for key in item4:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\": \"r\""
                attrs = attrs[2:]
                predicates = predicates + attrs + "}, "
            if len(item5) > 0:
                predicates = predicates + "\"PART\":{"
                attrs = ""
                for key in item5:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\": \"r\""
                attrs = attrs[2:]
                predicates = predicates + attrs + "}, "
            if len(item6) > 0:
                predicates = predicates + "\"PARTSUPP\":{"
                attrs = ""
                for key in item6:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\": \"r\""
                attrs = attrs[2:]
                predicates = predicates + attrs + "}, "
            if len(item7) > 0:
                predicates = predicates + "\"REGION\":{"
                attrs = ""
                for key in item7:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\": \"r\""
                attrs = attrs[2:]
                predicates = predicates + attrs + "}, "
            if len(item8) > 0:
                predicates = predicates + "\"SUPPLIER\":{"
                attrs = ""
                for key in item8:
                    attrs = attrs + ", \"" + key.split(".")[1] + "\": \"r\""
                attrs = attrs[2:]
                predicates = predicates + attrs + "}, "
            predicates = predicates[0:len(predicates) - 2]
            predicates += "}"
            query = "{\"id\":" + str(
                j + 23*num) + ", \"query_string\":\"" + sql[
                        j] + "\", \"predicates\":" + predicates + ", \"payload\":" + payload + ",  \"group_by\": {}, \"order_by\": {}}"
            queries.append(json.loads(query))
    return queries
        

def get_normalized(value, assumed_min, assumed_max, history_list):
    """
    This method gives a normalized reward based on the reward history

    :param value: current reward that we need to normalize
    :param history_list: rewards we got up to now, including the current reward
    :param assumed_min: assumed minimum value
    :param assumed_max: assumed maximum value
    :return: normalized reward (0 - 1)
    """
    if len(history_list) > 5:
        real_min = min(history_list) - 1
        real_max = max(history_list)
    else:
        real_min = min(min(history_list), assumed_min)
        real_max = max(max(history_list), assumed_max)
    return (value - real_min) / (real_max - real_min)


def update_dict_list(current, new):
    """
    This function does merging operation of 2 dictionaries with lists as values. This method adds only new values found
    in the new list to the old list

    :param current: current list
    :param new: new list
    :return: merged list
    """
    for table, predicates in new.items():
        if table not in current:
            current[table] = predicates
        else:
            temp_set = set(new[table]) - set(current[table])
            current[table] = current[table] + list(temp_set)
    return current


def plot_exp_report(exp_id, exp_report_list, measurement_names, log_y=False):
    """
    Creates a plot for several experiment reports
    :param exp_id: ID of the experiment
    :param exp_report_list: This can contain several exp report objects
    :param measurement_names: What measurement that we will use for y
    :param log_y: draw y axis in log scale
    """
    for measurement_name in measurement_names:
        comps = []
        final_df = DataFrame()
        for exp_report in exp_report_list:
            df = exp_report.data
            df[constants.DF_COL_COMP_ID] = exp_report.component_id
            final_df = pd.concat([final_df, df])
            comps.append(exp_report.component_id)

        final_df = final_df[final_df[constants.DF_COL_MEASURE_NAME] == measurement_name]
        # Error style = 'band' / 'bars'
        sns_plot = sns.relplot(x=constants.DF_COL_BATCH, y=constants.DF_COL_MEASURE_VALUE, hue=constants.DF_COL_COMP_ID,
                               kind="line", ci="sd", data=final_df, err_style="band")
        if log_y:
            sns_plot.set(yscale="log")
        plot_title = measurement_name + " Comparison"
        sns_plot.set(xlabel=constants.DF_COL_BATCH, ylabel=measurement_name)
        sns_plot.savefig(get_experiment_folder_path(exp_id) + plot_title + '.png')


def plot_exp_report_v2(exp_id, exp_report_list, measurement_names, log_y=False):
    """
    Creates a plot for several experiment reports
    :param exp_id: ID of the experiment
    :param exp_report_list: This can contain several exp report objects
    :param measurement_names: What measurement that we will use for y
    :param log_y: draw y axis in log scale
    """
    for measurement_name in measurement_names:
        comps = []
        final_df = DataFrame()
        for exp_report in exp_report_list:
            df = exp_report.data
            df[constants.DF_COL_COMP_ID] = exp_report.component_id
            final_df = pd.concat([final_df, df])
            comps.append(exp_report.component_id)

        final_df = final_df[final_df[constants.DF_COL_MEASURE_NAME] == measurement_name]
        # Error style = 'band' / 'bars'
        sns_plot = sns.relplot(x=constants.DF_COL_BATCH, y=constants.DF_COL_MEASURE_VALUE, hue=constants.DF_COL_COMP_ID,
                               kind="line", ci="sd", data=final_df, err_style="band")
        if log_y:
            sns_plot.set(yscale="log")
        plot_title = measurement_name + " Comparison"
        sns_plot.set(xlabel=constants.DF_COL_BATCH, ylabel=measurement_name)
        sns_plot.savefig(get_experiment_folder_path_v2(exp_id) + plot_title + '.png')


def plot_exp_report_v3(exp_id, probing_id, exp_report_list, measurement_names, log_y=False):
    """
    Creates a plot for several experiment reports
    :param exp_id: ID of the experiment
    :param exp_report_list: This can contain several exp report objects
    :param measurement_names: What measurement that we will use for y
    :param log_y: draw y axis in log scale
    """
    for measurement_name in measurement_names:
        comps = []
        final_df = DataFrame()
        final_df = final_df.reset_index(drop=True)
        for exp_report in exp_report_list:
            df = exp_report.data
            df[constants.DF_COL_COMP_ID] = exp_report.component_id
            final_df = pd.concat([final_df, df])
            comps.append(exp_report.component_id)

        final_df = final_df[final_df[constants.DF_COL_MEASURE_NAME] == measurement_name]
        # Error style = 'band' / 'bars'
        sns_plot = sns.relplot(x=constants.DF_COL_BATCH, y=constants.DF_COL_MEASURE_VALUE, hue=constants.DF_COL_COMP_ID,
                               kind="line", ci="sd", data=final_df, err_style="band")
        if log_y:
            sns_plot.set(yscale="log")
        plot_title = measurement_name + " Comparison"
        sns_plot.set(xlabel=constants.DF_COL_BATCH, ylabel=measurement_name)
        sns_plot.savefig(get_experiment_folder_path_v3(exp_id, probing_id) + plot_title + '.png')


def create_comparison_tables(exp_id, exp_report_list):
    """
    Create a CSV with numbers that are important for the comparison

    :param exp_id: ID of the experiment
    :param exp_report_list: This can contain several exp report objects
    :return:
    """
    final_df = DataFrame(
        columns=[constants.DF_COL_COMP_ID, constants.DF_COL_BATCH_COUNT, constants.MEASURE_HYP_BATCH_TIME,
                 constants.MEASURE_INDEX_RECOMMENDATION_COST, constants.MEASURE_INDEX_CREATION_COST,
                 constants.MEASURE_QUERY_EXECUTION_COST, constants.MEASURE_TOTAL_WORKLOAD_TIME])

    for exp_report in exp_report_list:
        data = exp_report.data
        component = exp_report.component_id
        rounds = exp_report.batches_per_rep
        reps = exp_report.reps

        # Get information from the data frame
        hyp_batch_time = get_avg_measure_value(data, constants.MEASURE_HYP_BATCH_TIME, reps)
        recommend_time = get_avg_measure_value(data, constants.MEASURE_INDEX_RECOMMENDATION_COST, reps)
        creation_time = get_avg_measure_value(data, constants.MEASURE_INDEX_CREATION_COST, reps)
        elapsed_time = get_avg_measure_value(data, constants.MEASURE_QUERY_EXECUTION_COST, reps)
        total_workload_time = get_avg_measure_value(data, constants.MEASURE_BATCH_TIME, reps) + hyp_batch_time

        # Adding to the final data frame
        final_df.loc[len(final_df)] = [component, rounds, hyp_batch_time, recommend_time, creation_time, elapsed_time,
                                       total_workload_time]

    final_df.round(4).to_csv(get_experiment_folder_path(exp_id) + 'comparison_table.csv')


# todo - remove min and max
def get_avg_measure_value(data, measure_name, reps):
    return (data[data[constants.DF_COL_MEASURE_NAME] == measure_name][constants.DF_COL_MEASURE_VALUE].sum())/reps


def get_sum_measure_value(data, measure_name):
    return data[data[constants.DF_COL_MEASURE_NAME] == measure_name][constants.DF_COL_MEASURE_VALUE].sum()


def change_experiment(exp_id):
    """
    Programmatically change the experiment

    :param exp_id: id of the new experiment
    """
    exp_config = configparser.ConfigParser()
    exp_config.read(constants.ROOT_DIR + constants.EXPERIMENT_CONFIG)
    exp_config['general']['run_experiment'] = exp_id
    with open(constants.ROOT_DIR + constants.EXPERIMENT_CONFIG, 'w') as configfile:
        exp_config.write(configfile)


def log_configs(logging, module):
    for variable in dir(module):
        if not variable.startswith('__'):
            logging.info(str(variable) + ': ' + str(getattr(module, variable)))
