### BALANCE: Boosting Index Advisor Learning with Multi-Source Workload Knowledge



```

.
│  box_line.pickle      # Predicate value box
│  main_PTF.py          # Main process file
│  README.md            # Code documentation
│  requirements.txt     # Experimental
│
├─balance
│      action_manager.py     
│      boo.py
│      configuration_parser.py
│      embedding_utils.py
│      experiment.py
│      observation_manager.py
│      reward_calculator.py
│      schema.py
│      utils.py
│      workload_embedder.py
│      workload_generator.py
│
├─experiments
│      tpcds.json         # setting files on TPCDS     
│      tpch.json          # setting files on TPCH
│
├─experiment_utils        # download on https://gitee.com/Liuhaoran233/balance_utils/tree/master/experiment_utils
│  │   box_line.pickle
│  │    
│  ├─cl_save              # cl model 
│  │  └─gen_model
│  │          boo_bao_nosame_f_v.pth   
│  │
│  ├─source               # transfer sources
│  │      f_s1.zip
│  │      f_s2.zip
│  │      f_s3.zip
│  │
│  └─workloads            # workloads on TPCH
│      └─gen_tpch
│              train_workloads1.pickle
│              train_workloads1_value.pickle
│
├─gym_db
│  │  common.py
│  │
│  └─envs
│         db_env_v1.py        # Reinforcement learning environment
│  
│
├─index_selection_evaluation  # database utils
├─query_files
│  ├─TPCDS
│  └─TPCH
├─src                         # value embedder
│  │  parameters.py
│  ├─ feature_extraction
│  ├─ plan_encoding
│  └─ token_embedding
│
└─stable_baselines            # Reinforcement learning Toolkit
   └─ ppo2            
           ppo2_PTF_w.py
```
### Example workflow
First, the files in folder ./experiment_utils need to be downloaded on https://gitee.com/Liuhaoran233/balance_utils/tree/master/experiment_utils
```
pip install -r requirements.txt         # Install requirements with pip
python main_PTF.py         # Run a experiment
```
