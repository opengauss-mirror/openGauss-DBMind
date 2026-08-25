# Distribution Key Recommendation
The distribution key (dk) recommendation function can be used in two scenarios: before or after data migration. The scenario before data migration refers to users migrating data from other databases (especially centralized databases, e.g., MySQL, PostgreSQL) to OpenGauss. This tool recommends the user the distribution keys that should be created under the OpenGauss for the given workload. The scenario after data migration refers to the process of tuning the existing distribution keys. For example, the performance with the existing database distribution key is not good. You can use the tool to recommend a better database distribution key configuration. The following demonstrates the specific usage methods.

Before data migration：

    python dk_advisor.py   [-f FILE] [-m MODE] [-s STATISTIC_INFO_FILE] [--dn DATA_NODE_NUM] [--min_distinct_threshold threshold] [--cost_type COST_TYPE]

e.g.,

    python dk_advisor.py -f ./test/tpch.sql --dn 6 -m offline -s ./test/statistics.json
 
After data migration：

    python dk_advisor.py   [-f FILE] [-p PORT] [-d DATABASE] [--schema SHEMA] [--host HOST] [-U USERNAME] [--dn DATA_NODE_NUM] [--start_time START_TIME] [--end_time END_TIME] [--min_distinct_threshold MINIMUM_DISTINCT_THRESHOLD] [--cost_type COST_TYPE] [--driver]

e.g.,
    
    python dk_advisor.py -m online --file test.sql -p 53400 -d bank --host linux173 -U dba --schema dams --dn 6
    # Or get from WDR
    python dk_advisor.py -m online --start_time 20210407 --end_time 20210408 -p 53400 -d bank -U dba --min_distinct_threshold 0.5 --dn 6 

Note: For "online" mode, please execute the "analyze" command before using this tool so that the database can obtain more accurate statistics.

# Parameter List and Description
| parameter name             | Description                                                  | Ranges             |
| -------------------------- | ------------------------------------------------------------ | ------------------ |
| MODE                       | Specify the current business scenario. The default mode is after data migration.  | offline,online |
| STATISTIC_INFO_FILE        | The path of the database statistics file in the before data migration scenario.   | -                  |
| FILE                       | The path of the file containing the statements of workload.  | -                  |
| DATABASE                   | The name of the connected database.                          | -                  |
| SCHEMA                     | Schema name                                                  | -                  |
| PORT                       | Port number to connect to the database.                      | -                  |
| DATA_NODE_NUM              | The number of data nodes after data migration.               | -                  |
| HOST                       | (Optional) Host address to connect to the database.          | -                  |
| USERNAME                   | (Optional) Username to connect to the database.              | -                  |
| PASSWORD                   | (Optional) Password for user to connect to the database. It can be read from pipe.    | -                  |
| START_TIME                 | (Optional) Start time of collecting WDR report business data.| yyyyMMdd           |
| END_TIME                   | (Optional) End time of collecting WDR report business data.  | yyyyMMdd           |
| MINIMUM_DISTINCT_THRESHOLD | (Optional) The minimum distinct value of the column.         | 0~1                |
| COST_TYPE                  | (Optional) Algorithm type for cost calculation. Default is naive.  | naive, optimizer   |
| MAX_REPLICATION_TABLE_SIZE | (Optional) Maximum number of rows in the replication table | 5e+05 |
| PRIOR_DISTRIBUTE_TRANSACTION | (Optional) Prioritize the processing of distributed transaction |  |
