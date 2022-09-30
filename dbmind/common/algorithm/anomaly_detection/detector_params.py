# According to actual experience, the predicted result
# will be more perfect after the amount of data
# (length of the sequence) exceeds a certain level,
# so we set a threshold here based on experience
# to decide different detection behaviors.

THRESHOLD = {
    "positive": (0.0, -float("inf")),
    "negative": (float("inf"), 0.0),
    "both": (-float("inf"), float("inf")),
    "neither": (float("inf"), -float("inf"))
}

WAIT_EVENT_GRAPH = {
    'os_cpu_usage': {
        'threshold': 0.6,
        'correlation': 0.2,
        'wait_events': [
            'analyze',
            'BufFileRead',
            'BufFileWrite',
            'BufMappingLock',
            'BufferIOLock',
            'BufHashTableSearch',
            'DataFileRead',
            'HashJoin - build hash',
            'Sort',
            'Sort - fetch tuple',
            'Sort - write file',
            'StrategyGetBuffer',
            'WALWriteLock'
        ],
        'other_metrics': [
            'os_mem_usage',
            'os_cpu_iowait',
            'os_disk_usage',
            'os_disk_iops',
            'os_disk_ioutils',
            'os_disk_iocapacity',
            'io_read_total',
            'io_read_bytes',
            'io_write_total',
            'io_write_bytes',
            'io_queue_number',
            'io_read_delay_time',
            'io_write_delay_time',
            'pg_database_all_size',
            'gaussdb_cpu_time',
            'gaussdb_state_memory',
            'gaussdb_data_file_read_time',
            'gaussdb_data_file_write_time',
            'gaussdb_confl_temp_bytes_rate',
            'gaussdb_confl_temp_files_rate',
            'gaussdb_connections_used_ratio',
        ]
    }
}
