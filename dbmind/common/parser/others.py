# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
import re

BLANK = ' '


def to_tuples(text):
    """Parse execution result by using gsql
     and convert to tuples."""
    lines = text.splitlines()
    separator_location = -1
    for i, line in enumerate(lines):
        # Find separator line such as '-----+-----+------'.
        if re.match(r'^\s*?[-|+]+\s*$', line):
            separator_location = i
            break

    if separator_location < 0:
        return []

    separator = lines[separator_location]
    left = 0
    right = len(separator)
    locations = list()
    while left < right:
        try:
            location = separator.index('+', left, right)
        except ValueError:
            break
        locations.append(location)
        left = location + 1
    # Record each value start location and end location.
    pairs = list(zip([0] + locations, locations + [right]))
    tuples = []
    row = []
    wrap_flag = False
    # Continue to parse each line.
    for line in lines[separator_location + 1:]:
        # Prevent from parsing bottom lines.
        if len(line.strip()) == 0 or re.match(r'\(\d+ rows?\)', line):
            continue
        # Parse a record to tuple.
        if wrap_flag:
            row[-1] += line[pairs[-1][0] + 1: pairs[-1][1]].strip()
        else:
            for start, end in pairs:
                # Increase 1 to start index to go over vertical bar (|).
                row.append(line[start + 1: end].strip())

        if len(line) == right and re.match(r'.*\s*\+$', line):
            wrap_flag = True
            row[-1] = row[-1].strip('+').strip(BLANK) + BLANK
        else:
            tuples.append(tuple(row))
            row = []
            wrap_flag = False
    return tuples


def parse_dsn(dsn):
    try:
        # Use psycopg2 first.
        from psycopg2.extensions import parse_dsn as parser
        from psycopg2 import ProgrammingError

        try:
            return parser(dsn)
        except ProgrammingError:
            # According to strict strategy, we have to ignore specific error
            # message.
            raise ValueError('Invalid dsn.') from None

    except ImportError:
        raise NotImplementedError()


def parse_mixed_quotes_string(input_string):
    # Regular expression to match quoted or unquoted strings
    pattern = r'"((?:[^"]|"")*)"|([^,]+)'

    # Find all matches in the input string
    matches = re.findall(pattern, input_string)

    # Process matches to handle quoted and unquoted values
    parsed_list = []
    for match in matches:
        if match[0]:  # This is a quoted string
            parsed_list.append(match[0].replace('""', '"'))
        else:  # This is an unquoted string
            parsed_list.append(match[1])

    return parsed_list


def extract_ip_groups(input_string):
    # Define a regex pattern to match key-value pairs
    input_string = input_string.strip(';')
    pattern = re.compile(r'''
        (?:
            # Match parts enclosed in quotes
            ['"]([^'"]+)['"]
        |
            # Match unquoted parts (cannot contain multiple colons)
            ([^:]+)
        )
        \s*:\s*
        (?:
            ['"]([^'"]+)['"]
        |
            ([^,:;]+)
        )
        ''', re.VERBOSE)
    
    # [] Support
    input_string = input_string.replace('[', '"').replace(']', '"')
    # Split the string by semicolons to separate groups
    groups = input_string.split(';')
    result = []

    for group in groups:
        group_results = []
        pairs = group.split(',')

        for pair in pairs:
            stripped_pair = pair.strip()

            # If not enclosed in quotes, check the count of colons
            if not (stripped_pair.startswith('"') or stripped_pair.startswith("'")):
                if stripped_pair.count(':') != 1:
                    return []  # Return an empty list if unquoted strings have more than one colon

            # Search for regex matches and parse
            match = pattern.fullmatch(stripped_pair)
            if match:
                key = match.group(1) or match.group(2)
                value = match.group(3) or match.group(4)
                group_results.append((key, value))
            else:
                return []  # Return an empty list if the format does not match

        # Add parsed results
        result.append(group_results)

    return result

