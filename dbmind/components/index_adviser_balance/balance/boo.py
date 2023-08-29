import re
from string import digits
from src.feature_extraction.predicate_features import *
from src.feature_extraction.extract_features import *


class BagOfOperators(object):

    def __init__(self):
        self.replacings = [(" ", ""), ("(", ""), (")", ""), ("[", ""),
                           ("]", ""), ("::text", "")]
        self.remove_digits = str.maketrans("", "", digits)
        self.INTERESTING_OPERATORS = [
            "Seq Scan",
            "Hash Join",
            "Nested Loop",
            "CTE Scan",
            "Index Only Scan",
            "Index Scan",
            "Merge Join",
            "Sort",
        ]

        self.relevant_operators = None
        self.relevant_values = None

    def value_from_plan(self, plan):
        self.relevant_values = []
        self._parse_value_plan(plan)

        return self.relevant_values

    def _parse_value_plan(self, plan):
        alias2table = {}
        get_alias2table(plan, alias2table)

        node_type = plan["Node Type"]
        if node_type in self.INTERESTING_OPERATORS:
            node_value_representation = self._parse_value_node(
                plan, alias2table)
            self.relevant_values.append(node_value_representation)
        if "Plans" not in plan:
            return
        for sub_plan in plan["Plans"]:
            self._parse_value_plan(sub_plan)

    def _parse_value_node(self, node, alias2table):
        relation_name, index_name = None, None
        if 'Relation Name' in node:
            relation_name = node['Relation Name']
        if 'Index Name' in node:
            index_name = node['Index Name']

        node_value_representation = None

        if node["Node Type"] == "Seq Scan":
            if 'Filter' in node:
                condition_seq_filter = pre2seq(node['Filter'], alias2table,
                                               relation_name, index_name)
                node_value_representation = get_value_reps_mean(
                    condition_seq_filter, relation_name, index_name)
        elif node["Node Type"] == "Index Only Scan":
            if 'Index Cond' in node:
                condition_seq_index = pre2seq(node['Index Cond'], alias2table,
                                              relation_name, index_name)
                node_value_representation = get_value_reps_mean(
                    condition_seq_index, relation_name, index_name)
        elif node["Node Type"] == "Index Scan":
            if 'Filter' in node:
                condition_seq_filter = pre2seq(node['Filter'], alias2table,
                                               relation_name, index_name)
            else:
                condition_seq_filter = []
            if 'Index Cond' in node:
                condition_seq_index = pre2seq(node['Index Cond'], alias2table,
                                              relation_name, index_name)
            else:
                condition_seq_index = []
            node_value_representation = get_value_reps_mean(
                condition_seq_filter + condition_seq_index, relation_name,
                index_name)
        elif node["Node Type"] == "CTE Scan":
            relation_name = node['CTE Name']
            if 'Filter' in node and node['Parent Relationship'] != 'Inner':
                condition_seq_filter = pre2seq(node['Filter'], alias2table,
                                               relation_name, index_name)
                node_value_representation = get_value_reps_mean(
                    condition_seq_filter, relation_name, index_name)
        return node_value_representation

    def boo_from_plan(self, plan):
        self.relevant_operators = []
        self._parse_plan(plan)

        return self.relevant_operators

    def _parse_plan(self, plan):
        node_type = plan["Node Type"]

        if node_type in self.INTERESTING_OPERATORS:
            node_representation = self._parse_node(plan)
            self.relevant_operators.append(node_representation)
        if "Plans" not in plan:
            return
        for sub_plan in plan["Plans"]:
            self._parse_plan(sub_plan)

    def _stringify_attribute_columns(self, node, attribute):
        attribute_representation = f"{attribute.replace(' ', '')}_"
        if attribute not in node:
            return attribute_representation

        value = node[attribute]

        for replacee, replacement in self.replacings:
            value = value.replace(replacee, replacement)

        value = re.sub('".*?"', "", value)
        value = re.sub("'.*?'", "", value)
        value = value.translate(self.remove_digits)

        return value

    def _stringify_list_attribute(self, node, attribute):
        attribute_representation = f"{attribute.replace(' ', '')}_"
        if attribute not in node:
            return attribute_representation

        assert isinstance(node[attribute], list)
        value = node[attribute]

        for element in value:
            attribute_representation += f"{element}_"

        return attribute_representation

    def _parse_bool_attribute(self, node, attribute):
        attribute_representation = f"{attribute.replace(' ', '')}_"

        if attribute not in node:
            return attribute_representation

        value = node[attribute]
        attribute_representation += f"{value}_"

        return attribute_representation

    def _parse_string_attribute(self, node, attribute):
        attribute_representation = f"{attribute.replace(' ', '')}_"

        if attribute not in node:
            return attribute_representation

        value = node[attribute]
        attribute_representation += f"{value}_"

        return attribute_representation

    def _parse_seq_scan(self, node):
        assert "Relation Name" in node

        node_representation = ""
        node_representation += f"{node['Relation Name']}_"

        node_representation += self._stringify_attribute_columns(
            node, "Filter")

        return node_representation

    def _parse_index_scan(self, node):
        assert "Relation Name" in node

        node_representation = ""
        node_representation += f"{node['Relation Name']}_"

        node_representation += self._stringify_attribute_columns(
            node, "Filter")
        node_representation += self._stringify_attribute_columns(
            node, "Index Cond")

        return node_representation

    def _parse_index_only_scan(self, node):
        assert "Relation Name" in node

        node_representation = ""
        node_representation += f"{node['Relation Name']}_"

        node_representation += self._stringify_attribute_columns(
            node, "Index Cond")

        return node_representation

    def _parse_cte_scan(self, node):
        assert "CTE Name" in node

        node_representation = ""
        node_representation += f"{node['CTE Name']}_"

        node_representation += self._stringify_attribute_columns(
            node, "Filter")

        return node_representation

    def _parse_nested_loop(self, node):
        node_representation = ""

        node_representation += self._stringify_attribute_columns(
            node, "Join Filter")

        return node_representation

    def _parse_hash_join(self, node):
        node_representation = ""

        node_representation += self._stringify_attribute_columns(
            node, "Join Filter")
        node_representation += self._stringify_attribute_columns(
            node, "Hash Cond")

        return node_representation

    def _parse_merge_join(self, node):
        node_representation = ""

        node_representation += self._stringify_attribute_columns(
            node, "Merge Cond")

        return node_representation

    def _parse_sort(self, node):
        node_representation = ""

        node_representation += self._stringify_list_attribute(node, "Sort Key")

        return node_representation

    def _parse_node(self, node):
        node_representation = f"{node['Node Type'].replace(' ', '')}_"

        if node["Node Type"] == "Seq Scan":
            node_representation += f"{self._parse_seq_scan(node)}"
        elif node["Node Type"] == "Index Only Scan":
            node_representation += f"{self._parse_index_only_scan(node)}"
        elif node["Node Type"] == "Index Scan":
            node_representation += f"{self._parse_index_scan(node)}"
        elif node["Node Type"] == "CTE Scan":
            node_representation += f"{self._parse_cte_scan(node)}"
        elif node["Node Type"] == "Nested Loop":
            node_representation += f"{self._parse_nested_loop(node)}"
        elif node["Node Type"] == "Hash Join":
            node_representation += f"{self._parse_hash_join(node)}"
        elif node["Node Type"] == "Merge Join":
            node_representation += f"{self._parse_merge_join(node)}"
        elif node["Node Type"] == "Sort":
            node_representation += f"{self._parse_sort(node)}"
        else:
            raise ValueError("_parse_node called with unsupported Node Type.")

        return node_representation
