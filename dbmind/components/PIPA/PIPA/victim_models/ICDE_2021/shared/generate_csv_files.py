import pickle
from importlib import reload

from pandas import DataFrame

import constants
from shared import configs_v2 as configs, helper

# Define Experiment ID list that we need to run
exp_id_list = ["tpc_h_skew_static_10_MAB3"]


def create_data_csv(exp_id, exp_report_list_inner):
    """
    Create a CSV with numbers that are important for the comparison

    :param exp_id: ID of the experiment
    :param exp_report_list_inner: This can contain several exp report objects
    :return:
    """
    separator = DataFrame({' ': ' ', 'Experiment': [exp_id]})
    separator['Experiment'].to_csv(helper.get_experiment_folder_path(exp_id) + 'data.csv', mode='w',
                                   index=False, header='True')
    for exp_report in exp_report_list_inner:
        data = exp_report.data
        component = exp_report.component_id
        separator = DataFrame({' ': ' ', 'Component Name': [component.replace(exp_id,'')]})

        # group by reps
        data = data.groupby(["Batch Number", "Measurement Name"]).mean().reset_index()
        data = data.pivot(index="Measurement Name", columns="Batch Number",values="Measurement Value").fillna(
            0.000000).round(6).rename(index={'Batch Time': 'Total Cost', 'Index Creation Time': 'Index Creation Cost'})
        if 'Index Recommendation Cost' not in data.index:
            recommendation_cost = data.loc['Total Cost'] - data.loc['Index Creation Cost'] - data.loc['Query Execution Cost']
            recommendation_cost.name = 'Index Recommendation Cost'
            data = data.append(recommendation_cost)
        if 'Memory Cost' in data.index:
            data = data.drop(['Memory Cost'])
        data['Total'] = data.sum(axis=1)
        separator['Component Name'].to_csv(helper.get_experiment_folder_path(exp_id) + 'data.csv', mode='a', index=False, header='True')
        data.to_csv(helper.get_experiment_folder_path(exp_id) + 'data.csv', mode='a', header='True')


for i in range(len(exp_id_list)):
    print(exp_id_list[i])
    experiment_folder_path = helper.get_experiment_folder_path(exp_id_list[i])
    helper.change_experiment(exp_id_list[i])
    reload(configs)

    with open(experiment_folder_path + "reports.pickle", "rb") as f:
        exp_report_list = pickle.load(f)
    create_data_csv(configs.experiment_id, exp_report_list)
