**The following is a specific configuration of the case study in the introduction section for the PIPA paper**

* **"dataset"** : **TPC-H** (The dataset for the experiment)
* **"data volume of dataset"** : **10gb**
* **"victim_model"** : **CIKM** ( the target model of the attack)
* **"attack_method"** : **PIPA** (the attack method for evaluation)
* **"poison_percentage"** : **1%** (the proportion of poisoned data in the total dataset for the experiment)
* **"exclude_template" : [2, 15, 17, 20]** (the numbers of template that are excluded in generating the normal workload)
* **"CIKM_parameters" :**
  * **"gen_new" : "True"**
  * **"model" :**
    * **"base" : "2020CIKM"**
    * **"algorithm" :**
      * **"constraint" : "index_number"**
      * **"parameters" :**
        * **"LR" : 0.02**
        * **"EPISILO" : 0.97**
        * **"Q_ITERATION" : 200**
        * **"U_ITERATION" : 10**
        * **"BATCH_SIZE" : 64**
        * **"GAMMA" : 0.95**
        * **"EPISODES" : 500**
        * **"LEARNING_START" : 150**
        * **"MEMORY_CAPACITY" : 20000**
        * **"number" : 1**
      * **"is_dnn" : "False"**
      * **"is_ps" : "True"**
      * **"is_double" : "True"**
  * **"probing" :**
    * **"epoches" : 200**
    * **"learning_start" : 100**
    * **"lr" : 0.02**
    * **"probing_num" : 20** (number of iterations of probing stage)
    * **"reward_lr" : 100**
  * **"generate_wl" :**
    * **"work_dir" :** the path of your 'tpch-kit/dbgen' file
    * **"w_size" :** "18"（the size of workload)
