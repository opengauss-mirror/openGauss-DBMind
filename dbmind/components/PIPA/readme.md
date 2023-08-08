# PIPA

Probing-Injecting Poisoning Attack Against Learning-based Index Advisors

### Code structure

.  
├── base_config.json　　　　　　　　　　　　　　　　　　　# Experimental configuration file  
├── main.py　　　　　　　　　　　　　　　　　　　　　　　# Main process file  
├── Readme.md　　　　　　　　　　　　　　　　　　　　　# Code documentation  
├── requirements.txt　　　　　　　　　　　　　　　　　　　# Experimental requirements file  
├── result　　　　　　　　　　　　　　　　　　　　　　　　# Experimental result directory  
├── victim_models　　　　　　　　　　　　　　　　　　　　# Victim index recommendation systems  
│　　　　├── CIKM_2020　　　　　　　　　　　　　　　　# You should add tpch/tpcds-kit under this directory  
│　　　　├── ICDE_2020　　　　　　　　　　　　　　　　　# You should add tpch/tpcds-kit under this directory  
│　　　　├── ICDE_2021  
│　　　　└── SWIRL_2022　　　　　　　　　　　　　　　　# You should add tpch/tpcds-kit under this directory  
└── workload_generation　　　　　　　　　　　　　　　　　# Workload generation module  
　　　　└── BartSqlGen　　　　　　　　　　　　　　　　　# IABART  
　　　　　　　　├── FSM　　　　　　　　　　　　　　　　　　　　# SQL automatic state machine  
　　　　　　　　├── local_transformers　　　　　　　　　　　　# Files about transformer  
　　　　　　　　├── model.py　　　　　　　　　　　　　　　　　# The main file of IABART  
　　　　　　　　├── net_full_TPCH.pth　　　　　　　　　　　　　　# Training result files on TPCH dataset  
　　　　　　　　├── net_full_TPCDS.pth　　　　　　　　　　　　　# Training result files on TPCDS dataset  
　　　　　　　　├── processing.py　　　　　　　　　　　　　　　　# Processing of original training data  
　　　　　　　　├── resource　　　　　　　　　　　　　　　　　　# The resource files required for training  
　　　　　　　　│　　　├── autocode.json  
　　　　　　　　│　　　├── config.json  
　　　　　　　　│　　　├── merges.txt  
　　　　　　　　│　　　├── pytorch_model.bin  
　　　　　　　　│　　　├── sql.jso　　　　　　　　　　　　　　　　# Your training data  
　　　　　　　　│　　　├── tokenizer.json  
　　　　　　　　│　　　└── vocab.json  
　　　　　　　　└── result　　　　　　　　　　　　　　　　　　　　# IABART training result files  


### Example workflow

```
pip install -r requirements.txt         # Install requirements with pip
python main.py base_config.json         # Run a experiment
```

Experiments can be controlled with the **base_config.json** file. For descriptions of the components and functioning, consult our paper.



### JSON Config files

All experimental hyperparameter configurations are stored in the **base_config.json** file. In the following, we explain the different configuration options:

* **experiments_id ( str )** : The name or identifier for the experiment. Output files are named accordingly.
* **experiments_root ( str )** : Absolute path of the experiment, used for locating.
* **victim_model ( str )** : Specify the target model of the attack, optional values are CIKM_2020/ICDE_2020/ICDE_2021/SWIRL_2022.
* **dataset ( str )** : Select the dataset for the experiment, optional types are tpch/tpcds.
* **result_path ( str )** : Relative path where the experimental results file is stored.
* **logging_path ( str )** : Relative path where the experimental log file is stored.
* **attack_method ( str )** : Specify the attack method for evaluation, optional values are: not_ood/bad/suboptimal/bad&suboptimal/random.
* **poison_percentage ( int )** : Specify the proportion of poisoned data in the total dataset for the experiment.
* **psql_connect** : Configure the parameters for connecting to a psql database.
  * **pg_ip ( str )** : The name of the psql database to be connected.
  * **pg_port ( str )** : The port number of the psql database to be connected.
  * **pg_user ( str )** : The username of the psql database to be connected.
  * **pg_password ( str )** : The password for the username of the psql database to be connected.