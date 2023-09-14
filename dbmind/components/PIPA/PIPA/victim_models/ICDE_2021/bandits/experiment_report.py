import pandas as pd
from pandas import DataFrame


class ExpReport:
    def __init__(self, exp_id, component_id, reps, batches_per_rep):
        """

        :param exp_id: id of the experiment
        :param component_id: what component are we testing
        :param reps: number of repetitions that we will repeat
        :param batches_per_rep: number of batches in a single test
        """
        self.exp_id = exp_id
        self.component_id = component_id
        self.reps = reps
        self.batches_per_rep = batches_per_rep
        self.data = DataFrame()      # A data frame
        self.total_workload_time = 0

    def add_data_tuple(self, data_tuple):
        """
        This will simply add the data tuple into the data list
        data tuple format (rep, batch_no, measurement_name, measurement)

        :param data_tuple: data collected from a batch
        """
        self.data.append(data_tuple)

    def add_data_list(self, data_list):
        """
        This will simply add the data tuple into the data list
        data tuple format rep, batch_no, measurement_name, measurement

        :param data_list: data collected from a repetition
        """
        self.data = pd.concat([self.data, data_list])
