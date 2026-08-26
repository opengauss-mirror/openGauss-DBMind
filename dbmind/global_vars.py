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
"""Using this global_vars must import as the following:

    >>> from dbmind import global_vars

Notice: The variables in the following should be used across files.
Otherwise, if a variable is not used across files,
it should be placed where the nearest used position is.
"""
# The following modules should be enough clean because
# this global_vars is an underlying module.

configs: "ReadonlyConfig" = None
dynamic_configs: "DynamicConfig" = None
metric_map = {}
metric_value_range_map = {}
must_filter_labels = {}
worker: "AbstractWorker" = None
confpath = ''
default_timed_task = []
timed_task = {}
is_dry_run_mode = False
is_distribute_mode = False
agent_proxy_setter: "AgentFactory" = None
agent_proxy: "AgentFactory.get_agent()" = None
metadatabase_list = []
ip_map = {}
LANGUAGE = 'en'
