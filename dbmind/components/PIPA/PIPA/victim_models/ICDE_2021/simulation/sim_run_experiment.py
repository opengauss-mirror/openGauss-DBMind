import pickle
from importlib import reload
import logging

from pandas import DataFrame

import constants
from bandits.experiment_report import ExpReport
from database.config_test_run import ConfigRunner
from database.dta_test_run_v2 import DTARunner
from shared import configs_v2 as configs, helper

# Define Experiment ID list that we need to run
exp_id_list = ["tpc_h_skew_static_10_MAB3"]

# Comparing components
OPTIMAL = constants.COMPONENT_OPTIMAL in configs.components
TA_OPTIMAL = constants.COMPONENT_TA_OPTIMAL in configs.components
TA_FULL = constants.COMPONENT_TA_FULL in configs.components
TA_CURRENT = constants.COMPONENT_TA_CURRENT in configs.components
TA_SCHEDULE = constants.COMPONENT_TA_SCHEDULE in configs.components
MAB = constants.COMPONENT_MAB in configs.components
NO_INDEX = constants.COMPONENT_NO_INDEX in configs.components
RL = constants.COMPONENT_DDQN in configs.components

# Generate form saved reports
FROM_FILE = False
SEPARATE_EXPERIMENTS = True
PLOT_LOG_Y = False
PLOT_MEASURE = (constants.MEASURE_BATCH_TIME, constants.MEASURE_QUERY_EXECUTION_COST,
                constants.MEASURE_INDEX_CREATION_COST)
UNIFORM = False

exp_report_list = []

for i in range(len(exp_id_list)):
    if SEPARATE_EXPERIMENTS:
        exp_report_list = []
    experiment_folder_path = helper.get_experiment_folder_path(exp_id_list[i])
    helper.change_experiment(exp_id_list[i])
    reload(configs)
    reload(logging)

    OPTIMAL = constants.COMPONENT_OPTIMAL in configs.components
    TA_OPTIMAL = constants.COMPONENT_TA_OPTIMAL in configs.components
    TA_FULL = constants.COMPONENT_TA_FULL in configs.components
    TA_CURRENT = constants.COMPONENT_TA_CURRENT in configs.components
    TA_SCHEDULE = constants.COMPONENT_TA_SCHEDULE in configs.components
    MAB = constants.COMPONENT_MAB in configs.components
    NO_INDEX = constants.COMPONENT_NO_INDEX in configs.components
    DDQN = constants.COMPONENT_DDQN in configs.components

    # configuring the logger
    if not FROM_FILE:
        logging.basicConfig(
            filename=experiment_folder_path + configs.experiment_id + '.log',
            filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
        logging.getLogger().setLevel(constants.LOGGING_LEVEL)

    if FROM_FILE:
        with open(experiment_folder_path + "reports.pickle", "rb") as f:
            exp_report_list = exp_report_list + pickle.load(f)
    else:
        print("Currently running: ", exp_id_list[i])
        # Running MAB
        if MAB:
            Simulators = {}
            for mab_version in configs.mab_versions:
                Simulators[mab_version] = (getattr(__import__(mab_version, fromlist=['Simulator']), 'Simulator'))
            for version, Simulator in Simulators.items():
                version_number = version.split("_v", 1)[1]
                exp_report_mab = ExpReport(configs.experiment_id,
                                           constants.COMPONENT_MAB + version_number + exp_id_list[i], configs.reps,
                                           configs.rounds)
                for r in range(configs.reps):
                    simulator = Simulator()
                    results, total_workload_time = simulator.run()
                    temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                       constants.DF_COL_MEASURE_VALUE])
                    temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                    temp[constants.DF_COL_REP] = r
                    exp_report_mab.add_data_list(temp)
                exp_report_list.append(exp_report_mab)

        # Running No Index
        if NO_INDEX:
            exp_report_no_index = ExpReport(configs.experiment_id, constants.COMPONENT_NO_INDEX + exp_id_list[i], configs.reps,
                                            configs.rounds)
            for r in range(configs.reps):
                results, total_workload_time = ConfigRunner.run("no_index.sql", uniform=UNIFORM)
                temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                   constants.DF_COL_MEASURE_VALUE])
                temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                temp[constants.DF_COL_REP] = r
                exp_report_no_index.add_data_list(temp)
            exp_report_list.append(exp_report_no_index)

        # Running Optimal
        if OPTIMAL:
            exp_report_optimal = ExpReport(configs.experiment_id, constants.COMPONENT_OPTIMAL + exp_id_list[i], configs.reps, configs.rounds)
            for r in range(configs.reps):
                results, total_workload_time = ConfigRunner.run("optimal_config.sql", uniform=UNIFORM)
                temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                   constants.DF_COL_MEASURE_VALUE])
                temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                temp[constants.DF_COL_REP] = r
                exp_report_optimal.add_data_list(temp)
            exp_report_list.append(exp_report_optimal)

        # Running DTA Optimal
        if TA_OPTIMAL:
            exp_report_ta = ExpReport(configs.experiment_id, constants.COMPONENT_TA_OPTIMAL + exp_id_list[i], configs.reps, configs.rounds)
            for r in range(configs.reps):
                dta_runner = DTARunner(configs.ta_runs, workload_type=constants.TA_WORKLOAD_TYPE_OPTIMAL)
                results, total_workload_time = dta_runner.run()
                temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                   constants.DF_COL_MEASURE_VALUE])
                temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                temp[constants.DF_COL_REP] = r
                exp_report_ta.add_data_list(temp)
            exp_report_list.append(exp_report_ta)

        # Running DTA Full
        if TA_FULL:
            exp_report_ta = ExpReport(configs.experiment_id, constants.COMPONENT_TA_FULL + exp_id_list[i], configs.reps,
                                      configs.rounds)
            for r in range(configs.reps):
                dta_runner = DTARunner([0], workload_type=constants.TA_WORKLOAD_TYPE_FULL)
                results, total_workload_time = dta_runner.run()
                temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                   constants.DF_COL_MEASURE_VALUE])
                temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                temp[constants.DF_COL_REP] = r
                exp_report_ta.add_data_list(temp)
            exp_report_list.append(exp_report_ta)

        # Running DTA Current
        if TA_CURRENT:
            exp_report_ta = ExpReport(configs.experiment_id, constants.COMPONENT_TA_CURRENT + exp_id_list[i],
                                      configs.reps, configs.rounds)
            for r in range(configs.reps):
                dta_runner = DTARunner(configs.ta_runs, workload_type=constants.TA_WORKLOAD_TYPE_CURRENT)
                results, total_workload_time = dta_runner.run()
                temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                   constants.DF_COL_MEASURE_VALUE])
                temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                temp[constants.DF_COL_REP] = r
                exp_report_ta.add_data_list(temp)
            exp_report_list.append(exp_report_ta)

        # Running DTA Schedule (everything from last run)
        if TA_SCHEDULE:
            exp_report_ta = ExpReport(configs.experiment_id, constants.COMPONENT_TA_SCHEDULE + exp_id_list[i],
                                      configs.reps, configs.rounds)
            for r in range(configs.reps):
                dta_runner = DTARunner(configs.ta_runs, workload_type=constants.TA_WORKLOAD_TYPE_SCHEDULE)
                results, total_workload_time = dta_runner.run()
                temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                   constants.DF_COL_MEASURE_VALUE])
                temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                temp[constants.DF_COL_REP] = r
                exp_report_ta.add_data_list(temp)
            exp_report_list.append(exp_report_ta)

        # Running DDQN
        if DDQN:
            from simulation.sim_ddqn_v3 import Simulator as DDQNSimulator
            exp_report_mab = ExpReport(configs.experiment_id, constants.COMPONENT_MAB + exp_id_list[i],
                                       configs.reps, configs.rounds)
            for r in range(configs.reps):
                simulator = DDQNSimulator()
                results, total_workload_time = simulator.run()
                temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                   constants.DF_COL_MEASURE_VALUE])
                temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                temp[constants.DF_COL_REP] = r
                exp_report_mab.add_data_list(temp)
            exp_report_list.append(exp_report_mab)

        # Save results
        with open(experiment_folder_path + "reports.pickle", "wb") as f:
            pickle.dump(exp_report_list, f)

        if SEPARATE_EXPERIMENTS:
            helper.plot_exp_report(configs.experiment_id, exp_report_list, PLOT_MEASURE, PLOT_LOG_Y)
            helper.create_comparison_tables(configs.experiment_id, exp_report_list)

# plot line graphs
if not SEPARATE_EXPERIMENTS:
    helper.plot_exp_report(configs.experiment_id, exp_report_list, PLOT_MEASURE, PLOT_LOG_Y)
    helper.create_comparison_tables(configs.experiment_id, exp_report_list)
