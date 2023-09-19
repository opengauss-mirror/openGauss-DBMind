import logging
import os
import sys
import json
CONFIGURATION_FILE = json.load(open(sys.argv[1]))

# ===============================  Program Related  ===============================
DB_CONFIG = '/config/db.conf'
EXPERIMENT_FOLDER = '/experiments'
WORKLOADS_FOLDER = '/resources/workloads'
EXPERIMENT_CONFIG = '/config/exp.conf'
ROOT_DIR = CONFIGURATION_FILE["experiments_root"] + '/victim_models/ICDE_2021'
LOGGING_LEVEL = logging.INFO

TABLE_SCAN_TIME_LENGTH = 1000

# ===============================  Database / Workload  ===============================
SCHEMA_NAME = 'dbo'

# ===============================  Arm Generation Heuristics  ===============================
INDEX_INCLUDES = 1
MAX_PERMUTATION_LENGTH = 2
SMALL_TABLE_IGNORE = 10000
TABLE_MIN_SELECTIVITY = 0.2
PREDICATE_MIN_SELECTIVITY = 0.01

# ===============================  Bandit Parameters  ===============================
ALPHA_REDUCTION_RATE = 1.05
QUERY_MEMORY = 1
BANDIT_FORGET_FACTOR = 0.6
MAX_INDEXES_PER_TABLE = 6
CREATION_COST_REDUCTION_FACTOR = 3
STOP_EXPLORATION_ROUND = 500
UNIFORM_ASSUMPTION_START = 10

# ===============================  Reward Related  ===============================
COST_TYPE_ELAPSED_TIME = 1
COST_TYPE_CPU_TIME = 2
COST_TYPE_SUB_TREE_COST = 3
COST_TYPE_CURRENT_EXECUTION = COST_TYPE_ELAPSED_TIME
COST_TYPE_CURRENT_CREATION = COST_TYPE_ELAPSED_TIME

# ===============================  Context Related  ===============================
CONTEXT_UNIQUENESS = 0
CONTEXT_INCLUDES = False
STATIC_CONTEXT_SIZE = 3

# ===============================  Reporting Related  ===============================
DF_COL_COMP_ID = "Component"
DF_COL_REP = "Rep"
DF_COL_BATCH = "Batch Number"
DF_COL_BATCH_COUNT = "# of Batches"
DF_COL_MEASURE_NAME = "Measurement Name"
DF_COL_MEASURE_VALUE = "Measurement Value"

MEASURE_TOTAL_WORKLOAD_TIME = "Total Workload Time"
MEASURE_INDEX_CREATION_COST = "Index Creation Time"
MEASURE_INDEX_RECOMMENDATION_COST = "Index Recommendation Cost"
MEASURE_QUERY_EXECUTION_COST = "Query Exc Time(unit：10000s)"
MEASURE_MEMORY_COST = "Memory Cost"
MEASURE_BATCH_TIME = "Batch Time"
MEASURE_HYP_BATCH_TIME = "Hyp Batch Time"

COMPONENT_MAB = "MAB"
COMPONENT_TA_OPTIMAL = "TA_OPTIMAL"
COMPONENT_TA_FULL = "TA_FULL"
COMPONENT_TA_CURRENT = "TA_CURRENT"
COMPONENT_TA_SCHEDULE = "TA_SCHEDULE"
COMPONENT_OPTIMAL = "OPTIMAL"
COMPONENT_NO_INDEX = "NO_INDEX"
COMPONENT_DDQN = "DDQN"
COMPONENT_DDQN_SINGLE_COLUMN = "DDQN_SINGLE_COLUMN"

TA_WORKLOAD_TYPE_OPTIMAL = 'optimal'
TA_WORKLOAD_TYPE_FULL = 'full'
TA_WORKLOAD_TYPE_CURRENT = 'current'
TA_WORKLOAD_TYPE_SCHEDULE = 'schedule'

# ===============================  Other  ===============================
TABLE_SCAN_TIMES = {"SSB": {"customer": [], "dwdate": [], "lineorder": [], "part": [], "supplier": []},
                    "tpch_1gb": {"LINEITEM": [], "CUSTOMER": [], "NATION": [], "ORDERS": [], "PART": [], "PARTSUPP": [],
                             "REGION": [], "SUPPLIER": []},
                    "TPCHSKEW": {"LINEITEM": [], "CUSTOMER": [], "NATION": [], "ORDERS": [], "PART": [], "PARTSUPP": [],
                                 "REGION": [], "SUPPLIER": []},
                    "TPCDS": {"call_center": [], "catalog_page": [], "catalog_returns": [], "catalog_sales": [],
                              "customer": [], "customer_address": [], "customer_demographics": [], "date_dim": [],
                              "dbgen_version": [], "household_demographics": [], "income_band": [], "inventory": [],
                              "item": [], "promotion": [], "reason": [], "ship_mode": [], "store": [],
                              "store_returns": [], "store_sales": [], "time_dim": [], "warehouse": [], "web_page": [],
                              "web_returns": [], "web_sales": [], "web_site": []},
                    "IMDB": {"aka_name": [], "aka_title": [], "cast_info": [], "char_name": [],
                             "comp_cast_type": [], "company_name": [], "company_type": [], "complete_cast": [],
                              "info_type": [], "keyword": [], "kind_type": [], "link_type": [],
                              "movie_companies": [], "movie_info": [], "movie_info_idx": [], "movie_keyword": [], "movie_link": [],
                              "name": [], "person_info": [], "role_type": [], "title": []}
                    }
