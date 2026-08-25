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

import string
import collections

from ..models.template_process import _is_punctuation, check_valid, check_valid_list

# 定义前缀树节点信息用于搜索路径
NodeArgs = collections.namedtuple(
    'NodeArgs',
    ['now', 'count', 'wd_count', 'dword_count', 'dword_index_first',
     'continue_step', 'continue_count', 'template', 'parameter']
)


class Trie:
    """
    功能描述：模板前缀树
    """

    def __init__(self, name='', dictionary=None, config=None):
        """
        功能描述：初始化树节点
        参数：
            name: 节点名字
            dictionary：合法词字典
            config: 参数
        """
        self.children = {}
        # 记录从根目录到该节点所对应的token序列对应的模板ID，-1代表无
        self.tag = -1
        # 记录当前节点的名字 token
        self.name = name

        # 初始化配置
        self.trie_config = config
        self.dictionary = dictionary

    def insert(self, template, template_id):
        """
        功能描述：添加模板到前缀树
        参数：
            template：分词后的模板
            template_id：模板ID
        """
        now = self
        for token in template:
            if token not in now.children:
                now.children[token] = Trie(name=token)
            now = now.children.get(token)
        now.tag = template_id

    def delete(self, template):
        """
        功能描述：删除前缀树里指定模板
        参数：
            template：需要删除的分词后的模板
        """
        n_token = len(template)
        node_list = [self]
        index = 0
        for index in range(n_token):
            node_list.append(node_list[index].children.get(template[index]))
            if not node_list[index + 1]:
                return None

        # 移除模板ID
        node_list[index + 1].tag = -1

        # 进一步检查是否能移除该模板路径其他节点
        for index in range(n_token, 0, -1):
            if node_list[index].children != {}:
                break
            del node_list[index - 1].children[template[index - 1]]

        return None

    def find(self, token_list):
        """
        功能描述：返回匹配到模板template的叶子节点对应的tag, 步骤如下：
            1、根据日志分词向量搜索叶子节点，通配符(匹配任意数字字母混合)可能会导致返回多个（候选）个叶子节点
            2、基于位置越前的合法词越重要的思想，候选叶子节点里优先选择最早匹配到合法词的模板
            3、若enable_match_more_wildcard为True，则在规则2基础上优先匹配通配符最多的模板，再其次是合法词最多的模板；
               若enable_match_more_wildcard为False，则在规则2基础上优先匹配合法词最多的模板，再其次是通配符最多的模板
            4、若匹配模板不包含合法词（即只有格式符、通配符或非合法词），但日志分词向量包含合法词，则返回-1
            5、max_wildcard_continue_step非零时允许在通配符的子节点无法匹配时候，该通配符能继续多匹配
                以wildcard_continue_delimiters代为间隔的非合法词或者少匹配非合法词的最大次数
            6、其他参数用于检查合法词
        参数：
            token_list：日志分词向量
        返回值：
            三元组包含：
                模板ID，-1表示无匹配
                模板分词列表
                模板参数列表
        """
        max_dword_index = 100
        stack = [NodeArgs(self, 0, 0, 0, max_dword_index, 0, 0, [], [])]
        result_match = {
            'dword_count': [],
            'score': [],
            'tag': [],
            'template': [],
            'parameter': []
        }

        self.traversal_stack(result_match, stack, token_list)

        if len(result_match.get('tag')) <= 0:
            return -1, [], []

        match_index = result_match.get('score').index(max(result_match.get('score')))
        if (
            result_match.get('dword_count')[match_index] == 0 and
            check_valid_list(token_list, self.dictionary, self.trie_config)
        ):
            return -1, [], []
        return (result_match.get('tag')[match_index],
                result_match.get('template')[match_index],
                result_match.get('parameter')[match_index])

    def traversal_stack(self, result_match, stack, token_list):
        """
        功能描述：遍历前缀树，寻找匹配模板template的叶子节点
        """
        while stack:
            stack_node = stack.pop()
            now, count, _, _, _, continue_step, _, _, _ = stack_node
            max_wildcard_continue_step = self.trie_config.get('max_wildcard_continue_step')
            if count >= len(token_list):
                if now.tag != -1:
                    self.match_now_node(stack_node, result_match)
                elif max_wildcard_continue_step > 0 and continue_step < max_wildcard_continue_step:
                    # 变长参数匹配: 模板参数数目比新日志多的情况
                    self.continue_match(stack, token_list, stack_node)
            else:
                # 模板参数数目比新日志少的情况
                stack = self._match_token(stack, token_list, stack_node)

    def _match_token(self, stack, token_list, stack_node):
        """
        功能描述：匹配token，对通配符、标点等进行处理，模板参数数目比新日志少的情况
        """
        (now, count, wd_count, dword_count, dword_index_first, continue_step,
         continue_count, template, parameter) = stack_node
        max_wildcard_continue_step = self.trie_config.get('max_wildcard_continue_step')
        wildcard = self.trie_config.get('wildcard')
        token = token_list[count]
        if token == wildcard:
            # 通配符只能精确匹配
            if wildcard in now.children:
                node = now.children.get(wildcard)
                stack.append((node, count + 1, wd_count + 1, dword_count, dword_index_first,
                              0, continue_count, template + [wildcard], parameter + [token]))
        elif all(char in string.punctuation + ' ' for char in token):
            stack = self.match_token(stack_node, max_wildcard_continue_step, token, token_list, stack)
        else:
            stack = self.match_not_wildcard_token(stack_node, wildcard, token, stack)
        return stack

    def match_now_node(self, stack_node, result_match):
        """
        功能描述：匹配到当前节点的情况
        """
        (now, count, wd_count, dword_count, dword_index_first, continue_step,
         continue_count, template, parameter) = stack_node
        result_match['tag'].append(now.tag)
        result_match['dword_count'].append(dword_count)
        result_match['template'].append(template)
        result_match['parameter'].append(parameter)
        if self.trie_config.get('enable_match_more_wildcard'):
            result_match['score'].append(-100 * dword_index_first + wd_count
                                         + dword_count * 0.01 + continue_count * 0.001)
        else:
            result_match['score'].append(-100 * dword_index_first + dword_count
                                         + wd_count * 0.01 + continue_count * 0.001)

    def match_not_wildcard_token(self, stack_node, wildcard, token, stack):
        """
        功能描述：匹配非通配符与非纯格式符情形, 可与自身或者通配符匹配
        """
        (now, count, wd_count, dword_count, dword_index_first, continue_step,
         continue_count, template, parameter) = stack_node
        if wildcard in now.children:
            node = now.children[wildcard]
            stack.append((node, count + 1, wd_count + 1, dword_count, dword_index_first,
                          0, continue_count, template + [wildcard], parameter + [token]))

        if token in now.children:
            node = now.children[token]
            if check_valid(token, self.dictionary, self.trie_config):
                stack.append((node, count + 1, wd_count, dword_count + 1, min(dword_index_first, count),
                              0, continue_count, template + [token], parameter))
            else:
                stack.append((node, count + 1, wd_count, dword_count, dword_index_first,
                              0, continue_count, template + [token], parameter))
        return stack

    def match_token(self, stack_node, max_wildcard_continue_step, token, token_list, stack):
        """
        功能描述：匹配格式符的情形
        """
        (now, count, wd_count, dword_count, dword_index_first, continue_step,
         continue_count, template, parameter) = stack_node
        # 纯格式符只能精确匹配, 除非是参数间隔符，且允许通配符连续匹配
        if token in now.children:
            node = now.children[token]
            stack.append((node, count + 1, wd_count, dword_count, dword_index_first,
                          0, continue_count, template + [token], parameter))
        # 变长参数匹配
        if max_wildcard_continue_step > 0 and continue_step < max_wildcard_continue_step:
            self.continue_match(stack, token_list, NodeArgs(now, count, wd_count, dword_count, dword_index_first,
                                                            continue_step, continue_count, template, parameter))
        return stack

    def continue_match(self, stack, token_list, stack_node):
        """
        功能描述：变长参数匹配
                模板参数数目比新日志少的情况, 若前缀树当前节点为通配符, 分词为变长参数间隔符，
                且下个分词不是：（1）纯字母+连接符的合法词，（2）纯格式符（除通配符以外）
                则前缀树位置不变，输入日志位置向前两步, 通配符连续匹配时不能匹配合法词
        """
        node_consts = NodeArgs(stack_node)
        wildcard = self.trie_config.get('wildcard')
        wildcard_continue_delimiters = self.trie_config.get('wildcard_continue_delimiters')
        if node_consts.now.name != wildcard:
            return

        if (node_consts.count + 1) < len(token_list):
            node_token = token_list[node_consts.count]
            next_node_token = token_list[node_consts.count + 1]
            if (node_token in wildcard_continue_delimiters and \
                not check_valid(next_node_token, self.dictionary, self.trie_config)) \
                    and (next_node_token == wildcard or not _is_punctuation(next_node_token)):
                stack.append((node_consts.now, node_consts.count + 2,
                              node_consts.wd_count + 1, node_consts.dword_count,
                              node_consts.dword_index_first, node_consts.continue_step + 1,
                              node_consts.continue_count + 1, node_consts.template,
                              node_consts.parameter + [next_node_token]))
        else:
            # 模板参数数目比新日志多的情况，若当前前缀树节点是通配符，子节点包含变长参数分隔符，
            # 且该子节点的子节点包含通配符, 则前缀树向下两层，输入日志位置不变
            for continue_delimiter in wildcard_continue_delimiters:
                child_continue_delimiter = node_consts.now.children.get(continue_delimiter)
                if child_continue_delimiter and (wildcard in child_continue_delimiter.children):
                    node = child_continue_delimiter.children[wildcard]
                    stack.append((node, node_consts.count, node_consts.wd_count,
                                  node_consts.dword_count, node_consts.dword_index_first,
                                  node_consts.continue_step + 1, node_consts.continue_count + 1,
                                  node_consts.template + [continue_delimiter, wildcard],
                                  node_consts.parameter))
