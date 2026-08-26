"""
Metric mapping for DBMind to actual Prometheus metrics
Maps expected metric names to actual queries that work with opengauss_exporter
"""

# Mapping of DBMind expected metrics to actual Prometheus queries
METRIC_MAPPING = {
    # Connection metrics - using actual available metrics
    'gaussdb_total_connection': 'pg_connections_used_conn',
    'gaussdb_active_connection': 'pg_connections_used_conn',  # Use total connections as active (simplified)
    'gaussdb_idle_connection': 'pg_connections_idle_session',
    
    # Alternative connection metric names
    'pg_connections_active': 'pg_connections_used_conn',  # Use total connections as active (simplified)
    'pg_connections_idle': 'pg_connections_idle_session',
    'pg_connections_total': 'pg_connections_used_conn',
    'pg_connections_waiting': 'pg_connections_enqueue_sql',
    'pg_connections_idle_in_transaction': 'pg_connections_idle_session',
    
    # Use existing connection metrics directly
    'pg_connections_used_conn': 'pg_connections_used_conn',
    'pg_connections_max_conn': 'pg_connections_max_conn',
    'pg_connections_used_rate': 'pg_connections_used_rate',
    
    # Transaction metrics
    'pg_transactions_commit': 'pg_db_xact_commit',
    'pg_transactions_rollback': 'pg_db_xact_rollback',
    'pg_transactions_running': 'pg_connections_used_conn',  # Use total connections as running transactions (simplified)
    
    # Performance metrics
    'pg_database_size': 'pg_database_size_bytes',
    'pg_locks_count': 'pg_db_confl_lock',
    'pg_deadlocks_count': 'pg_db_deadlocks',
    
    # Cache hit ratio - use a simple metric instead of calculation
    'pg_cache_hit_ratio': 'pg_db_blks_hit',  # Simplified - just use hit count
    'pg_db_blks_access': 'pg_db_blks_access',
    
    # Database metrics
    'pg_db_xact_commit': 'pg_db_xact_commit',
    'pg_db_xact_rollback': 'pg_db_xact_rollback',
    'pg_db_blks_read': 'pg_db_blks_read',
    'pg_db_blks_hit': 'pg_db_blks_hit',
    'pg_db_tup_returned': 'pg_db_tup_returned',
    'pg_db_tup_fetched': 'pg_db_tup_fetched',
    'pg_db_tup_inserted': 'pg_db_tup_inserted',
    'pg_db_tup_updated': 'pg_db_tup_updated',
    'pg_db_tup_deleted': 'pg_db_tup_deleted',
    
    # Keep these as-is since they may exist
    'pg_sql_count_dcl': 'pg_sql_count_dcl',
    'pg_sql_count_ddl': 'pg_sql_count_ddl', 
    'pg_sql_count_dml': 'pg_sql_count_dml',
    'pg_sql_count_select': 'pg_sql_count_select',
    'pg_sql_count_update': 'pg_sql_count_update',
    'pg_sql_count_insert': 'pg_sql_count_insert',
    'pg_sql_count_delete': 'pg_sql_count_delete',
    
    # Response time metrics - may not exist, keep as-is for now
    'statement_responsetime_percentile_p80': 'statement_responsetime_percentile_p80',
    'statement_responsetime_percentile_p95': 'statement_responsetime_percentile_p95',
}

def map_metric_name(metric_name):
    """
    Map a DBMind metric name to the actual Prometheus query
    Returns the mapped query or the original metric name if no mapping exists
    """
    return METRIC_MAPPING.get(metric_name, metric_name)

def is_complex_query(metric_name):
    """
    Check if the metric requires a complex Prometheus query (not just a simple metric name)
    """
    mapped = METRIC_MAPPING.get(metric_name, metric_name)
    # Check for operators that indicate a complex query
    return any(op in mapped for op in ['sum(', 'avg(', 'max(', 'min(', '/', '+', '-', '*', '{', '}'])