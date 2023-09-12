# An Interpretable What-If Estimator for Database Knobs


## Table of contents

1. [Main Modules](#modules)
2. [Benchmark](#benchmark)
3. [Run](#run)


## Main Modules <a name="modules"></a>

| Module                    | Description                                                                                   |
|---------------------------|-----------------------------------------------------------------------------------------------|
| knob_tool                 | Knob management tools, including database restart, configuration update, etc.                 |
| knob_evaluator            | Model implementation and historical experience reuse.                                         |
| collect                   | Collect performance data.                                                                     |
| rank                      | Sort by Knob Importance data.                                                                 |
| main                      | Task startup and parameter parsing.                                                           |


## Benchmark <a name="benchmark"></a>

We use the open-source database benchmarking tool [benchbase](https://github.com/cmu-db/benchbase) for database performance testing and data collection.

You need to download the repo and compile benchbase, then add the tool path into config.yaml

## Run <a name="run"></a>

```
usage: An Efficient Estimation System for the Knob Tuning under Dynamic Workload [-h] [--config [CONFIG]] [--collect] [--two_stage] [--rank] [--evaluate] [--train] [--save] [--use_exp]

optional arguments:
  -h, --help            show this help message and exit
  --config [CONFIG], -c [CONFIG]
                        config file used to connect database and execute workload
  --collect             execute collect command
  --two_stage           collect performance data using two-stage strategy
  --rank                execute rank command
  --evaluate            evaluate two sets of knobs
  --train               train model for knob performance evaluation
  --save                save current experience to pool
  --use_exp             utilize historical experience for current workload

Nice:)
```

An example for direct learning, containing the data collection, knob importance ranking and model training.

```shell
# collect performance data under different knob config.
gs_dbmind component knob_estimator --config config.yaml --collect
# rank knobs by their importance.
gs_dbmind component knob_estimator --config config.yaml --rank
# train model to predict database's performance given a knob config.
gs_dbmind component knob_estimator --config config.yaml --train
```

Also, IWEK could implement the transfer learning to obtain the knob estimator from historical experiences. First remove the model_path in config.yaml and run the following command, the program will automatically reuse the history experience, and obtain the tuning recommendation from IWEK.

```shell

gs_dbmind component knob_estimator --config config.yaml --evaluate
```


