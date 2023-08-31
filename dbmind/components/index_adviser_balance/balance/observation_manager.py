import logging
import numpy as np
from gym import spaces
from index_selection_evaluation.selection.dbms.openguass_dbms import OpenguassDatabaseConnector
from src.feature_extraction.extract_features import *

from src.parameters import *
from src.plan_encoding.meta_info import *
from balance.utils import *

VERY_HIGH_BUDGET = 100_000_000_000


class ObservationManager(object):

    def __init__(self, number_of_actions):
        self.number_of_actions = number_of_actions

    def _init_episode(self, state_fix_for_episode):
        self.episode_budget = state_fix_for_episode["budget"]
        if self.episode_budget is None:
            self.episode_budget = VERY_HIGH_BUDGET

        self.initial_cost = state_fix_for_episode["initial_cost"]

    def init_episode(self, state_fix_for_episode):
        raise NotImplementedError

    def get_observation(self, environment_state):
        raise NotImplementedError

    def get_observation_space(self):
        observation_space = spaces.Box(low=self._create_low_boundaries(),
                                       high=self._create_high_boundaries(),
                                       shape=self._create_shape())

        logging.info(
            f"Creating ObservationSpace with {self.number_of_features} features."
        )

        return observation_space

    def _create_shape(self):
        return (self.number_of_features, )

    def _create_low_boundaries(self):
        low = [-np.inf for feature in range(self.number_of_features)]

        return np.array(low)

    def _create_high_boundaries(self):
        high = [np.inf for feature in range(self.number_of_features)]

        return np.array(high)


class EmbeddingObservationManager(ObservationManager):

    def __init__(self, number_of_actions, config):
        ObservationManager.__init__(self, number_of_actions)

        self.workload_embedder = config["workload_embedder"]
        self.representation_size = self.workload_embedder.true_representation_size
        self.workload_size = config["workload_size"]

        self.number_of_features = (
            self.
            number_of_actions  
            + (self.representation_size * self.workload_size
               )  
            + self.
            workload_size  
            + 1  
            + 1  
            + 1  
            + 1  
        )

    def _init_episode(self, state_fix_for_episode):
        episode_workload = state_fix_for_episode["workload"]
        self.frequencies = np.array(
            EmbeddingObservationManager._get_frequencies_from_workload(
                episode_workload))

        super()._init_episode(state_fix_for_episode)

    def init_episode(self, state_fix_for_episode):
        raise NotImplementedError

    def get_observation(self, environment_state):
        if self.UPDATE_EMBEDDING_PER_OBSERVATION:
            workload_embedding = np.array(
                self.workload_embedder.get_embeddings(
                    environment_state["plans_per_query"]))
        else:
            if self.workload_embedding is None:
                self.workload_embedding = np.array(
                    self.workload_embedder.get_embeddings(
                        environment_state["plans_per_query"]))

            workload_embedding = self.workload_embedding

        observation = np.array(environment_state["action_status"])
        observation = np.append(observation, workload_embedding)
        observation = np.append(observation, self.frequencies)
        observation = np.append(observation, self.episode_budget)
        observation = np.append(
            observation, environment_state["current_storage_consumption"])
        observation = np.append(observation, self.initial_cost)
        observation = np.append(observation, environment_state["current_cost"])

        return observation

    @staticmethod
    def _get_frequencies_from_workload(workload):
        frequencies = []
        for query in workload.queries:
            frequencies.append(query.frequency)
        return frequencies


# Todo: Rename. Single/multi-column is not handled by the ObservationManager anymore.
# All managers are capable of handling single and multi-attribute indexes now.
class SingleColumnIndexWorkloadEmbeddingObservationManager(
        EmbeddingObservationManager):

    def __init__(self, number_of_actions, config):
        super().__init__(number_of_actions, config)

        self.UPDATE_EMBEDDING_PER_OBSERVATION = False

    def init_episode(self, state_fix_for_episode):
        super()._init_episode(state_fix_for_episode)

        self.workload_embedding = np.array(
            self.workload_embedder.get_embeddings(
                state_fix_for_episode["workload"]))


# Todo: Rename. Single/multi-column is not handled by the ObservationManager anymore.
# All managers are capable of handling single and multi-attribute indexes now.
class SingleColumnIndexPlanEmbeddingObservationManager(
        EmbeddingObservationManager):

    def __init__(self, number_of_actions, config):
        super().__init__(number_of_actions, config)

        self.UPDATE_EMBEDDING_PER_OBSERVATION = True

    def init_episode(self, state_fix_for_episode):
        super()._init_episode(state_fix_for_episode)


# Todo: Rename. Single/multi-column is not handled by the ObservationManager anymore.
# All managers are capable of handling single and multi-attribute indexes now.
class SingleColumnIndexPlanEmbeddingObservationManagerWithoutPlanUpdates(
        EmbeddingObservationManager):

    def __init__(self, number_of_actions, config):
        super().__init__(number_of_actions, config)

        self.UPDATE_EMBEDDING_PER_OBSERVATION = False

    def init_episode(self, state_fix_for_episode):
        super()._init_episode(state_fix_for_episode)

        self.workload_embedding = None





# Todo: Rename. Single/multi-column is not handled by the ObservationManager anymore.
# All managers are capable of handling single and multi-attribute indexes now.
class SingleColumnIndexPlanEmbeddingObservationManagerWithCost(
        EmbeddingObservationManager):

    def __init__(self, number_of_actions, config):
        super().__init__(number_of_actions, config)

        self.UPDATE_EMBEDDING_PER_OBSERVATION = True
        self.db_connector = self.workload_embedder.database_connector
        self.parameters = self.getParameters()

        print("use boo")
        # This overwrites EmbeddingObservationManager's features
        self.number_of_features = (
            self.
            number_of_actions  # Indicates for each action whether it was taken or not
            + (self.representation_size * self.workload_size
               )  # embedding representation for the workload
            + self.workload_size  # The costs for every query in the workload
            + self.
            workload_size  # The frequencies for every query in the workloads
            + 1  # The episode's budget
            + 1  # The current storage consumption
            + 1  # The initial workload cost
            + 1  # The current workload cost
        )

    def getParameters(self):
        column2pos, tables_id, columns_id, physic_ops_id, compare_ops_id, bool_ops_id, tables, columnTypeisNum, box_lines = prepare_dataset(
        )
        table_total_num = len(tables_id)
        column_total_num = len(columns_id)
        physic_op_total_num = len(physic_ops_id)
        compare_ops_total_num = len(compare_ops_id)
        bool_ops_total_num = len(bool_ops_id)
        box_num = 10
        condition_op_dim = bool_ops_total_num + compare_ops_total_num + column_total_num + box_num

        parameters = Parameters(tables_id, columns_id, physic_ops_id,
                                column_total_num, table_total_num,
                                physic_op_total_num, condition_op_dim,
                                compare_ops_id, bool_ops_id,
                                bool_ops_total_num, compare_ops_total_num,
                                box_num, columnTypeisNum, box_lines)
        return parameters

    def init_episode(self, state_fix_for_episode):
        super()._init_episode(state_fix_for_episode)

    def w_get_detail_repr(self, w):
        ids = []
        fr = []
        texts = []
        for query in w.queries:
            ids.append(query.nr)
            fr.append(query.frequency)
            texts.append(query.text)
        return f"Query IDs: {ids} with {fr}. {w.description} Budget: None Detail: {texts}"

    def get_observation(self, environment_state, dn=None):

        workload_embedding = np.array(
            self.workload_embedder.get_embeddings(
                environment_state["plans_per_query"]))

        observation = np.array(environment_state["action_status"])
        observation = np.append(observation, workload_embedding)
        observation = np.append(observation,
                                environment_state["costs_per_query"])
        observation = np.append(observation, self.frequencies)
        observation = np.append(observation, self.episode_budget)
        observation = np.append(
            observation, environment_state["current_storage_consumption"])
        observation = np.append(observation, self.initial_cost)
        observation = np.append(observation, environment_state["current_cost"])

        return observation


class SingleColumnIndexObservationManager(ObservationManager):

    def __init__(self, number_of_actions, config):
        ObservationManager.__init__(self, number_of_actions)

        self.number_of_query_classes = config["number_of_query_classes"]

        self.number_of_features = (
            self.
            number_of_actions  # Indicates for each action whether it was taken or not
            + self.
            number_of_query_classes  # The frequencies for every query class
            + 1  # The episode's budget
            + 1  # The current storage consumption
            + 1  # The initial workload cost
            + 1  # The current workload cost
        )

    def init_episode(self, state_fix_for_episode):
        episode_workload = state_fix_for_episode["workload"]
        super()._init_episode(state_fix_for_episode)
        self.frequencies = np.array(
            self._get_frequencies_from_workload_wide(episode_workload))

    def get_observation(self, environment_state):
        observation = np.array(environment_state["action_status"])
        observation = np.append(observation, self.frequencies)
        observation = np.append(observation, self.episode_budget)
        observation = np.append(
            observation, environment_state["current_storage_consumption"])
        observation = np.append(observation, self.initial_cost)
        observation = np.append(observation, environment_state["current_cost"])

        return observation

    def _get_frequencies_from_workload_wide(self, workload):
        frequencies = [0 for query in range(self.number_of_query_classes)]

        for query in workload.queries:
            frequencies[query.nr - 1] = query.frequency

        return frequencies
