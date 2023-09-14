import logging
import sys
import json
import os
import time
import copy
from pathlib import Path

logging.basicConfig(
    filename=json.load(open(sys.argv[1]))["logging_path"]+'/'+json.load(open(sys.argv[1]))["experiments_id"]+'.log',
    format = '%(filename)s %(levelname)s %(message)s',
    level=logging.INFO)
logging.info("####====Process Begin====####")


if __name__ == "__main__":
    assert len(sys.argv) == 2, logging.error("Experiment configuration file must be provided: main.py base_config.json")
    CONFIGURATION_FILE = json.load(open(sys.argv[1]))
    config = copy.deepcopy(CONFIGURATION_FILE)

    logging.info("Load Configuration File Successful.")

    if CONFIGURATION_FILE["victim_model"] == "CIKM_2020":
        from victim_models.CIKM_2020.main import CIKM_2020
        logging.info("Choose CIKM_2020 as the victim model.")
        report = list()
        i = 0
        while i < 5:
            CONFIGURATION_FILE["experiments_id_exp"] = CONFIGURATION_FILE["experiments_id"] + "/" + "exp" + str(i)
            CIKM = CIKM_2020(CONFIGURATION_FILE)

            logging.warning("=====Prepare Begin======")
            CIKM.prepare()
            logging.warning("=====Prepare End======")

            logging.warning("=====Train Victim Model Begin======")
            CIKM.train()
            logging.warning("=====Train Victim Model End======")

            logging.warning("=====Probing Information from Victim Model Begin======")
            CIKM.probing()
            logging.warning("=====Probing Information from Victim Model End======")

            logging.warning("=====Attack Victim Model Begin======")
            CIKM.poison()
            logging.warning("=====Attack Victim Model End======")

            logging.warning("=====Evaluate Attack Influence Begin======")
            best_reward_bias, avg_reward_bias, vmf, best_bias_ratio, avg_bias_ratio = CIKM.evaluation()
            logging.warning("=====Evaluate Attack Influence End======")

            report.append([best_reward_bias, avg_reward_bias, vmf, best_bias_ratio, avg_bias_ratio])
            logging.warning("best_reward_bias,avg_reward_bias,vmf,best_bias_ratio,avg_bias_ratio: " + str(
                [best_reward_bias, avg_reward_bias, vmf, best_bias_ratio, avg_bias_ratio]))

            if i != 4:
                del (CIKM)

            i = i + 1
            CONFIGURATION_FILE = copy.deepcopy(config)

        logging.warning("=====Output Result Begin======")
        CIKM.finish(report)
        logging.warning("=====Output Result End======")

    if CONFIGURATION_FILE["victim_model"] == "ICDE_2020":
        from victim_models.ICDE_2020.main import ICDE_2020
        logging.info("Choose ICDE_2020 as the victim model.")
        report = list()
        i = 0
        while i < 5:
            CONFIGURATION_FILE["experiments_id_exp"] = CONFIGURATION_FILE["experiments_id"] + "/" + "exp" + str(i)
            ICDE = ICDE_2020(CONFIGURATION_FILE)

            logging.warning("=====Prepare Begin======")
            ICDE.prepare()
            logging.warning("=====Prepare End======")

            logging.warning("=====Train Victim Model Begin======")
            ICDE.train()
            logging.warning("=====Train Victim Model End======")

            logging.info("=====Probing Information from Victim Model Begin======")
            ICDE.probing_v2()
            logging.info("=====Probing Information from Victim Model End======")

            logging.warning("=====Attack Victim Model Begin======")
            ICDE.poison()
            logging.warning("=====Attack Victim Model End======")

            logging.warning("=====Evaluate Attack Influence Begin======")
            best_reward_bias, avg_reward_bias, vmf, best_bias_ratio, avg_bias_ratio = ICDE.evaluation()
            logging.warning("=====Evaluate Attack Influence End======")

            report.append([best_reward_bias, avg_reward_bias, vmf, best_bias_ratio, avg_bias_ratio])
            logging.warning("best_reward_bias,avg_reward_bias,vmf,best_bias_ratio,avg_bias_ratio: " + str(
                [best_reward_bias, avg_reward_bias, vmf, best_bias_ratio, avg_bias_ratio]))

            if i != 4:
                del (ICDE)

            if vmf <= 1:
                continue

            i = i + 1
            CONFIGURATION_FILE = copy.deepcopy(config)

        logging.warning("=====Output Result Begin======")
        ICDE.finish(report)
        logging.warning("=====Output Result End======")

    if CONFIGURATION_FILE["victim_model"] == "ICDE_2021":
        from victim_models.ICDE_2021.main import ICDE_2021
        logging.info("Choose ICDE_2021 as the victim model.")
        report = list()
        i = 0
        while i < 5:
            CONFIGURATION_FILE["experiments_id_exp"] = CONFIGURATION_FILE["experiments_id"] + "/" + "exp" + str(i)        
            ICDE = ICDE_2021(CONFIGURATION_FILE)

            logging.info("=====Prepare Begin======")
            ICDE.prepare()
            logging.info("=====Prepare End======")

            logging.info("=====Train Victim Model Begin======")
            ICDE.train()
            logging.info("=====Train Victim Model End======")

            logging.info("=====Probing Information from Victim Model Begin======")
            ICDE.probing_v2()
            logging.info("=====Probing Information from Victim Model End======")

            logging.info("=====Attack Victim Model Begin======")
            best_reward_bias, avg_reward_bias, vmf, best_bias_ratio, avg_bias_ratio = ICDE.poison()
            logging.info("=====Attack Victim Model End======")

            logging.info("=====Evaluate Attack Influence Begin======")
            ICDE.evaluation()
            logging.info("=====Evaluate Attack Influence End======")

            report.append([best_reward_bias, avg_reward_bias, vmf, best_bias_ratio, avg_bias_ratio])
            logging.warning("best_reward_bias,avg_reward_bias,vmf,best_bias_ratio,avg_bias_ratio: " + str(
                [best_reward_bias, avg_reward_bias, vmf, best_bias_ratio, avg_bias_ratio]))

            if i != 4:
                del (ICDE)
                
            if vmf <= 1:
                continue

            i = i + 1
            CONFIGURATION_FILE = copy.deepcopy(config)

        logging.info("=====Output Result Begin======")
        ICDE.finish(report)
        logging.info("=====Output Result End======")

    if CONFIGURATION_FILE["victim_model"] == "SWIRL_2022":
        from victim_models.SWIRL_2022.main import SWIRL_2022
        logging.info("Choose SWIRL_2020 as the victim model.")
        report = list()
        i = 0
        while i < 5:  
            SWIRL = SWIRL_2022(CONFIGURATION_FILE)
            SWIRL.prepare()
            logging.info("=====Prepare End======")

            logging.info("=====Train Victim Model Begin======")
            SWIRL.train()
            logging.info("=====Train Victim Model End======")

            logging.info("=====Probing Information from Victim Model Begin======")
            SWIRL.probing()
            logging.info("=====Probing Information from Victim Model End======")

            logging.info("=====Attack Victim Model Begin======")
            SWIRL.poison()
            logging.info("=====Attack Victim Model End======")

            logging.info("=====Evaluate Attack Influence Begin======")
            SWIRL.evaluation()
            logging.info("=====Evaluate Attack Influence End======")

            if i != 4:
                del (SWIRL)

            if vmf <= 1:
                continue

            i = i + 1

        logging.info("=====Output Result Begin======")
        SWIRL.finish(report)
        logging.info("=====Output Result End======")







