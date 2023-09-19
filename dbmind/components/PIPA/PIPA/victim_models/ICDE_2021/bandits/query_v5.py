import database.sql_helper_v2 as sql_helper


class Query:
    def __init__(self, connection, query_id, query_string, predicates, payloads, time_stamp=0):
        self.id = query_id
        self.predicates = predicates
        self.payload = payloads
        self.group_by = {}
        self.order_by = {}
        self.selectivity = sql_helper.get_selectivity_v3(connection, query_string, self.predicates)
        self.query_string = query_string
        self.frequency = 1
        self.last_seen = time_stamp
        self.first_seen = time_stamp
        self.table_scan_times = sql_helper.get_table_scan_times_structure()
        self.index_scan_times = sql_helper.get_table_scan_times_structure()
        self.table_scan_times_hyp = sql_helper.get_table_scan_times_structure()
        self.index_scan_times_hyp = sql_helper.get_table_scan_times_structure()
        self.context = None

    def __hash__(self):
        return self.id

    def get_id(self):
        return self.id
