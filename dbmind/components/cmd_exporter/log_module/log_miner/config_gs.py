# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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

log_template_config = {
    # 替换的模板通配符
    'wildcard': '<*>',
    # 用于去噪的日志文本预处理正则表达式列表
    'noise_masking_instructions': [],
    # 业务自定义日志关键词
    'words_add': ('postgres', 'postgresql', 'template0', 'template1', 'templatem', 'templatea', 'template_pdb'
                  'obs', 'walreceiver', 'microseconds', 'username/password', 'lsn', 'dcf', 'segno'),
    # 业务自定义日志过滤词列表
    'words_drop': (),
    # 最大分词数量
    'max_token_size': 500,
    # 合法词的最小长度
    'min_word_length': 2,
    # 合法词的最大长度
    'max_word_length': 50,
    # 是否考虑复合词的合法性
    'enable_compose': True,
    # 推理时是否考虑不在字典的复合词是合法的情形
    'enable_compose_valid_check': True,
    # 日志分词的分隔符列表
    'delimiters': list(' ,;:=|@()[]{}\'\"!<>.'),
    # 复合词的连接词列表，默认包含下划线
    'compose_delimiters': ('_', '-', '/'),
    # 复合词里合法词的最小长度
    'min_compose_word_length': 3,
    # 日志内容差异是否区分大小写
    'case_sensitive': False,
    # 判断在最早匹配合法词相同情况下，优先返回通配符最多的模板，开启即支持变长变量的日志模板提取
    'enable_match_more_wildcard': False,
    # 允许在通配符的子节点无法匹配时候，该通配符能继续多匹配多少次非合法词或者少匹配多少次非合法词
    'max_wildcard_continue_step': 0,
    'wildcard_continue_delimiters': (' ', ','),
    # 是否使得含有中文的分词必为关键词
    'force_chinese_valid': False,
    # 是否提取参数，默认为True, 开启会增加一定空间开销
    'enable_parameter_extraction': True,
}

log_formats = {
    "postgresql": (
        '<date> <time> <node_name> <user_name> <datname> <host> '
        '<thread_id> <session_id> <trans_id> <app_name> <query_id> '
        '\\[<thread_name>] <level>: <content>'
    ),
    "opengauss": (
        '<date> <time> <node_name> <user_name> <datname> <host> '
        '<thread_id> <session_id> <trans_id> <app_name> <query_id> '
        '\\[<thread_name>] <level>: <content>'
    ),
    "cm_agent": "<date> <time> tid=<tid> <app_name> <level>: <content>",
    "cm_server": "<date> <time> tid=<tid> <app_name> <level>: <content>",
    "system_call": (
        '<date> <time> <node_name> <user_name> <datname> <host> '
        '<thread_id> <session_id> <trans_id> <app_name> <query_id> '
        '\\[<thread_name>] <level>: <content>'
    ),
    "gtm": "<a>:<b>:<date> <time> -<content>",
    "ffic_opengauss": "",
    "system_alarm": "<content>"
}

log_labels = {
    # aggregation label is the sub set of log format
    # there must be a 'content' in the label
    "postgresql": ["node_name", "user_name", "datname", "host", "app_name", "thread_name", "content"],
    "opengauss": ["node_name", "user_name", "datname", "host", "app_name", "thread_name", "content"],
    "cm_agent": ["content"],
    "cm_server": ["content"],
    "system_call": ["app_name", "content"],
    "gtm": ["content"],
    "system_alarm": ["content"]
}
