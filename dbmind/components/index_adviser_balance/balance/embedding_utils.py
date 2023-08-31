import logging
import random
import numpy as np
from .boo import BagOfOperators


def which_queries_to_remove(plans,
                            queries_to_remove,
                            seed=None,
                            experiment_id=None,
                            excluded_query_classes=frozenset([]),
                            experiment_folder_path=None):
    random.seed(seed)

    all_operators = set()
    relevant_operators_wo_indexes = []
    relevant_operators_with_indexes = []
    boo_creator = BagOfOperators()

    for plan in plans[0]:
        boo = boo_creator.boo_from_plan(plan)
        relevant_operators_wo_indexes.append(boo)
        all_operators |= set(boo)

    for plan in plans[1]:
        boo = boo_creator.boo_from_plan(plan)
        relevant_operators_with_indexes.append(boo)
        all_operators |= set(boo)

    idx_without_removals = []
    for idx, (op_wo, op_with) in enumerate(
            zip(relevant_operators_wo_indexes,
                relevant_operators_with_indexes)):
        operators_combined = set(op_wo) | set(op_with)

        operators_without_q = set()
        for idx2, (op_wo, op_with) in enumerate(
                zip(relevant_operators_wo_indexes,
                    relevant_operators_with_indexes)):
            if idx2 == idx:
                continue
            operators_without_q |= set(op_wo)
            operators_without_q |= set(op_with)

        operators_exclusive_to_q = all_operators - operators_without_q
        operators_exclusive_to_q_2 = operators_combined - operators_without_q

        assert operators_exclusive_to_q == operators_exclusive_to_q_2

        if len(operators_exclusive_to_q) == 0:
            idx_without_removals.append(idx)

    if queries_to_remove <= len(idx_without_removals):
        for i in range(10_000):
            remove = random.sample(idx_without_removals, queries_to_remove)
            new_ops = set()
            for idx, (op_wo, op_with) in enumerate(
                    zip(relevant_operators_wo_indexes,
                        relevant_operators_with_indexes)):
                if idx in remove:
                    continue
                operators_combined = set(op_wo) | set(op_with)
                new_ops |= operators_combined
            if len(all_operators - new_ops) == 0:
                for idx in range(len(remove)):
                    remove[idx] += 1
                return remove

    all_idx = frozenset([i for i in range(len(plans[0]))])
    current_best_sample = {"indices": [], "unique_operators": 0}

    zero_indexed_excluded = frozenset([i - 1 for i in excluded_query_classes])

    for i in range(50_000):
        remove = random.sample(all_idx - zero_indexed_excluded,
                               queries_to_remove)
        new_ops = set()
        for idx, (op_wo, op_with) in enumerate(
                zip(relevant_operators_wo_indexes,
                    relevant_operators_with_indexes)):
            if idx in remove:
                continue
            operators_combined = set(op_wo) | set(op_with)
            new_ops |= operators_combined
        if len(new_ops) > current_best_sample["unique_operators"]:
            current_best_sample["unique_operators"] = len(new_ops)
            current_best_sample["indices"] = remove

            print(
                f"new best removal: removing {len(all_operators - new_ops)} operators"
            )

            jk = experiment_folder_path + "/best_" + experiment_id + ".txt"
            np.savetxt(jk, [len(all_operators - new_ops)], fmt='%d')

    remove = current_best_sample["indices"]
    for idx in range(len(remove)):
        remove[idx] += 1

    logging.critical(
        f"Removing the following operators: {all_operators - new_ops}")

    return remove
