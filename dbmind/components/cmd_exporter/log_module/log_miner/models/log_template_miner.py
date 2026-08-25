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

import json
import logging as logger

from . import wordninja
from ..models.trie import Trie
from ..models.template_process import (
    preprocess_noise,
    tokenize,
    _is_punctuation,
    _check_chinese,
    _preprocess_dictionary,
    _maskdel_wildcard,
    _check_template_simplest,
    _get_compose_type,
    _get_template
)


class LogTemplateMiner:
    """
    功能描述：基于字典集的日志模板挖掘方法, 字典集定义了常量的可能值集合，模板ID从1开始计数
    参数：
        dictionary：日志合法词字典（集合或列表格式, 以常见英文单词字典为基准）
        config: 日志模板挖掘模型预处理参数（字典格式）
        verbosity：是否打印执行过程日志，默认为0，不打印，1会打印步骤，大于1显示进度条
    """

    def __init__(self, dictionary=None,
                 config=None,
                 verbosity=0):

        # initialize config
        self.config = self.initialize_config()

        # read config
        if config is not None:
            self.config.update(config)

        # process config
        self.config['delimiters'] = set(self.config['delimiters'])
        self.config['compose_delimiters'] = set(self.config['compose_delimiters'])
        self.config['word_strip'] = ''.join(
            set('#$%"\'(),\|:;!?@[]^{}') - self.config.get('delimiters') - self.config.get(
                'compose_delimiters') - set(self.config.get('wildcard')))

        # check config
        if (not self.config.get('enable_compose')) and self.config.get('enable_compose_valid_check'):
            logger.warning('Set config [enable_compose_valid_check] to False because [enable_compose] is False!')
            self.config['enable_compose_valid_check'] = False

        # process dictionary
        dictionary = set() if dictionary is None else set(dictionary)
        self.dictionary = _preprocess_dictionary(dictionary.copy(),
                                                 self.config.get('words_add'),
                                                 self.config.get('words_drop'))

        self.verbosity = verbosity

        # 初始化日志解析前缀树与模板字典
        self.template_max_id = 0
        self.dwords_to_id = {}
        self.id_to_dwords = {}
        self.templates_dict = {}
        self.trie = Trie(dictionary=self.dictionary, config=self.config)

    @staticmethod
    def initialize_config():
        config_default = {
            # 替换的模板通配符
            'wildcard': '<*>',
            # 用于去噪的日志文本预处理正则表达式列表
            'noise_masking_instructions': [],
            # 业务自定义日志关键词
            'words_add': (),
            # 业务自定义日志过滤词列表
            'words_drop': (),
            # 最大分词数量
            'max_token_size': 500,
            # 合法词的最小长度
            'min_word_length': 3,
            # 合法词的最大长度
            'max_word_length': 50,
            # 是否考虑复合词的合法性
            'enable_compose': True,
            # 推理时是否考虑不在字典的复合词是合法的情形
            'enable_compose_valid_check': True,
            # 日志分词的分隔符列表
            'delimiters': list(' ,;:=|@()[]{}\'\"!<>./'),
            # 复合词的连接词列表，默认包含下划线
            'compose_delimiters': ('_',),
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
        return config_default

    def save_model_json(self, file_path, with_dictionary=True, encoding='utf-8', ensure_ascii=False):
        """
        功能描述：存储模型参数为json格式
        参数：
            file_path：存储的文件目录
            with_dictionary：是否存储合法词字典，默认为True
            encoding：存储的文本编码，默认为'utf-8'
            ensure_ascii：json存储是否确保ascii编码，默认为False，中文可以正常显示
        """
        config = self.config
        config['delimiters'] = list(config.get('delimiters'))
        config['compose_delimiters'] = list(config.get('compose_delimiters'))
        model_parameters = {'dictionary': list(self.dictionary) if with_dictionary else [],
                            'config': config,
                            'template_max_id': self.template_max_id,
                            'id_to_dwords': self.id_to_dwords,
                            'templates_dict': self.templates_dict,
                            }
        with open(file_path, 'w+', encoding=encoding) as file:
            json.dump(model_parameters, file, ensure_ascii=ensure_ascii)

    def load_model_json(self, file_path, with_dictionary=True, encoding='utf-8'):
        """
        功能描述：载入json格式的模型参数
        参数：
            file_path：读取的文件目录
            with_dictionary：是否载入合法词字典，默认为True
            encoding：读取的文本编码，默认为'utf-8'
        """
        with open(file_path, 'r', encoding=encoding) as file:
            model_parameters = json.load(file)
        if with_dictionary:
            self.dictionary = set(model_parameters.get('dictionary', []))
        self.config = model_parameters.get('config', {})
        self.config['delimiters'] = set(self.config.get('delimiters'))
        self.config['compose_delimiters'] = set(self.config.get('compose_delimiters'))
        self.template_max_id = model_parameters.get('template_max_id')
        self.id_to_dwords = model_parameters.get('id_to_dwords')
        self.templates_dict = model_parameters.get('templates_dict')

        # rebuild dwords_to_id
        id_to_dwords = {}
        self.dwords_to_id = {}
        for key, dwords_list in self.id_to_dwords.items():
            id_to_dwords[key] = [tuple(dwords) for dwords in dwords_list]
            for dwords_tuple in id_to_dwords.get(key):
                self.dwords_to_id[dwords_tuple] = key
        self.id_to_dwords = id_to_dwords

        # rebuild trie tree
        self.rebuild_trie()

    def group_by_dict(self, log_messages_proc):
        """
        功能描述：根据合法词字典对日志分词向量提取合法词特征向量，进而根据合法词特征向量聚类。
            具体为合法词特征向量第一个元素代表日志记录分词数量，从第二个元素开始拼接日志行分词向量，规则如下：
            0、首个分词，若其为纯字母，且长度在min_word_length和max_word_length之间，则加入合法词字典
            1. 若分词为纯格式符，则加入合法词特征向量
            2. 若分词符为纯数字，则往合法词特征向量加入通配符
            3. 若分词长度在在min_word_length和max_word_length之外，则往合法词特征向量加入通配符
            4. 若force_chinese_valid为True，则中文分词直接加入合法词特征向量
            5. 若分词在合法词字典存在，则加入合法词特征向量
            6. 若分词由若干合法词拼接而成（允许有数字和给定连接词compose_delimiters），则该分词为复合词。
                wordninja将复合词拆散后如果含有长度大于等于min_compose_word_length的合法词，
                则将此复合词包含的合法词拼接，并加入合法词特征向量和合法词字典
            7. 其余情况，往合法词特征向量加入通配符
        参数：
            log_messages_proc: 已分词处理后的日志内容列表
        返回值：
            合法词向量聚类结果
        """
        self.dictionarize(log_messages_proc)

        # 根据字典对每个日志提取合法词特征向量，同时动态更新字典
        dicted_list = self.lexicalized_groupby_general(log_messages_proc)

        # 根据合法词特征向量聚类group by wordset
        dwords_group = {}
        for element in dicted_list:
            frozen_dwords = tuple(element['dwords'])
            if frozen_dwords not in dwords_group:
                dwords_group[frozen_dwords] = []
            dwords_group.get(frozen_dwords).append(element)

        result_group = {}
        result_key = 1
        for key in dwords_group.keys():
            result_group[result_key] = dwords_group.get(key)
            result_key += 1
        return result_group

    def dictionarize(self, log_messages_proc):
        """
        将首个分词合法词添加到字典中
        """

        for wordlist in log_messages_proc:
            if (
                wordlist and
                self.config.get('min_word_length') <= len(wordlist[0]) <= self.config.get('max_word_length') and
                wordlist[0].isalpha()
            ):
                self.dictionary.add(wordlist[0])

    def lexicalized_groupby_general(self, log_messages_proc):
        """
        功能描述：日志词汇化
        """
        result = list()
        min_word_length = self.config.get('min_word_length')
        max_word_length = self.config.get('max_word_length')
        force_chinese_valid = self.config.get('force_chinese_valid')

        # dictionarized
        for line_id, wordlist in enumerate(log_messages_proc):
            len_wordlist = len(wordlist)
            wordset = [0] * (len_wordlist + 1)
            wordset[0] = len_wordlist
            for index, word in enumerate(wordlist):
                wordset[index + 1] = self.config.get('wildcard')
                # 判断词类属性
                if (
                    _is_punctuation(word) or  # 是标点
                    _check_chinese(word, force_chinese_valid) or  # 是中文
                    word in self.dictionary  # 在字典中
                ):  # 保持原样
                    wordset[index + 1] = word
                elif (
                    not (min_word_length <= len(word) <= max_word_length) or  # 长度太长或者太短
                    any(char.isdigit() for char in word)  # 包含数字
                ):  # 替换为通配符
                    wordset[index + 1] = self.config.get('wildcard')
                elif self.config.get('enable_compose'):  # 复合词检测
                    compose_type = _get_compose_type(word, self.config.get('compose_delimiters'))
                    self._lexicalized_compose_word(word, wordset, index, compose_type)

            result_dict = dict(message=wordlist, dwords=wordset, line_id=line_id)
            result.append(result_dict)

        return result

    def _lexicalized_compose_word(self, word, wordset, index, compose_type):
        """
        功能描述：考虑复合词的合法性，提取合法词特征向量，同时动态更新字典
        """
        if compose_type == 0:
            return
        elif compose_type == 2:
            wordset[index + 1] = self.config.get('wildcard')
            return

        splitted_words = wordninja.split(word)
        exist_invalid_compose = False
        for splitted_word in splitted_words:
            if (
                len(splitted_word) < self.config.get('min_compose_word_length') or
                splitted_word not in self.dictionary
            ):
                exist_invalid_compose = True

        # 纯字母+复合词连接词
        if not exist_invalid_compose:
            self.dictionary.add(word)
            wordset[index + 1] = word
        else:
            wordset[index + 1] = self.config.get('wildcard')

    def build_log_template(self, dictionarize_clusters):
        """
        功能描述：对每个合法词向量的聚类提取模板，建立合法词向量到模板的映射，并根据前缀树对合法词向量到模板的映射提取公共前缀压缩
        参数：
            dictionarize_clusters：合法词向量聚类结果
        返回值：
            更新输入的dictionarize_clusters，以及class的以下四个参数：
                trie：prefix tree用来压缩template内容以及在在线推理阶段查找模板
                dwords_to_id：dword to template_id, 1对1
                id_to_dwords：template_id to dword, 1对多
                templates_dict：template_id to template, 包含template和count, 允许不同template_id的template一样
        """
        self.dwords_to_id = {}
        self.id_to_dwords = {}
        self.templates_dict = {}

        # 对每个聚类提取模板
        for key in dictionarize_clusters:
            sorted_list = [x['message'] for x in dictionarize_clusters[key]]
            template = sorted_list[0]
            for seq_other in sorted_list[1:]:
                template = _get_template(template, seq_other, self.config.get('wildcard'))
            self.templates_dict[key] = {'template': template, 'count': len(sorted_list)}
            frozen_dwords = tuple(dictionarize_clusters[key][0]['dwords'])
            self.dwords_to_id[frozen_dwords] = key
            self.id_to_dwords[key] = [frozen_dwords]

        # 检查模板是不是最简化
        for _, entry in self.templates_dict.items():
            entry['simplest'] = _check_template_simplest(entry['template'], self.config.get('wildcard'),
                                                         self.dictionary)

        # 添加模板到前缀树, 并根据前缀树匹配结果合并合法词特征
        self.rebuild_trie(dictionarize_clusters)

    def rebuild_trie(self, dictionarize_clusters=None):
        """
        功能描述：根据已有模板字典，重构模板前缀树，清除冗余模板
        参数：
            dictionarize_clusters：合法词向量聚类结果，默认为空，若不为空则根据前缀树进行更新
        """
        enable_dictionarize_clusters = False if not dictionarize_clusters else True
        self.trie = Trie(dictionary=self.dictionary, config=self.config)
        templates_list = sorted(self.templates_dict.items(),
                                key=lambda temp: _maskdel_wildcard(temp[1]['template'], self.config.get('wildcard')))
        # building trie tree
        for entry in templates_list:
            template = entry[1]['template']
            key = entry[0]
            tag, _, _ = self.trie.find(template)

            if tag == -1:
                self.trie.insert(template, key)
            elif tag != key:
                self.templates_dict[tag]['count'] += self.templates_dict.get(key, {}).get('count', 0)
                self.templates_dict.pop(key)
                self.id_to_dwords[tag].extend(self.id_to_dwords.get(key))
                for dwords in self.id_to_dwords.get(key, {}):
                    self.dwords_to_id[dwords] = tag
                self.id_to_dwords.pop(key)
                if enable_dictionarize_clusters:
                    dictionarize_clusters[tag].extend(dictionarize_clusters.get(key))
                    dictionarize_clusters.pop(key)

        logger.info(f'Finished building template trie tree, remain {len(self.templates_dict.keys())} bin(s)')

    def train_model(self, log_messages, return_train_template=False):
        """
        功能描述：训练日志模板挖掘模型
        参数：
            log_messages：日志内容列表
        返回值：
            输入数据对应的日志模板ID、模板化内容、日志原本内容、该模板出现次数组成的字典的列表。
            如果enable_parameter_extraction为True，则字典里还包括变量。
        """
        # 预处理日志数据
        log_messages_proc = preprocess_noise(log_messages,
                                             self.config.get('noise_masking_instructions'),
                                             self.config.get('case_sensitive'))
        logger.info('Finished preprocessing.')
        log_messages_proc = tokenize(log_messages_proc,
                                     self.config.get('max_token_size'),
                                     self.config.get('word_strip'),
                                     self.config.get('delimiters'),
                                     self.config.get('wildcard'))
        logger.info('Finished tokenization.')
        # 根据字典对每个日志提取合法词，进而根据合法词特征向量聚类
        dictionarize_clusters = self.group_by_dict(log_messages_proc)
        self.template_max_id = len(dictionarize_clusters)

        # 对每个合法词向量的聚类提取模板，建立合法词向量到模板的映射，并根据前缀树对合法词向量到模板的映射提取公共前缀压缩
        self.build_log_template(dictionarize_clusters)

        # 提取输入日志的模板
        log_templates = None
        if return_train_template:
            log_templates = [None] * len(log_messages)
            for key in dictionarize_clusters.keys():
                for rs in dictionarize_clusters.get(key):
                    template = ''.join(self.templates_dict[self.dwords_to_id[tuple(rs['dwords'])]]['template'])
                    count = self.templates_dict.get(self.dwords_to_id.get(tuple(rs['dwords'])), {}).get('count')
                    log_templates[rs['line_id']] = {'template_id': key,
                                                    'template': template,
                                                    'parameter': None,
                                                    'content': log_messages[rs['line_id']],
                                                    'count': count}
        logger.info('Finished training template miner.')

        return log_templates

    def predict(self, log_messages_new):
        """
        功能描述：推理日志模板
        参数：
            log_messages_new：新的日志内容列表
        返回值：
            log_templates_new：输入数据对应的日志模板ID、模板化内容、日志原本内容、该模板出现次数组成的字典的列表
            novel_increment_dict：新出现的日志模板的字典
        """
        log_messages_proc = preprocess_noise(log_messages_new,
                                             self.config.get('noise_masking_instructions'),
                                             self.config.get('case_sensitive'))
        log_messages_proc = tokenize(log_messages_proc,
                                     self.config.get('max_token_size'),
                                     self.config.get('word_strip'),
                                     self.config.get('delimiters'),
                                     self.config.get('wildcard'))
        log_templates_new = [None] * len(log_messages_proc)
        num_novel = 0
        novel_increment_dict = {}

        # predict/infer by trie tree
        for index in range(len(log_messages_proc)):
            message = log_messages_proc[index]

            # 从前缀树查找模板，并返回结果
            tag, template, novel_increment_dict, num_novel, count, parameter = self.find_trie(
                message, novel_increment_dict, num_novel)

            log_templates_new[index] = {
                'template_id': tag,
                'template': template,
                'parameter': None,
                'content': log_messages_new[index],
                'count': count
            }
            if self.config.get('enable_parameter_extraction'):
                log_templates_new[index]['parameter'] = parameter

        return log_templates_new, novel_increment_dict

    def find_trie(self, message, novel_increment_dict, num_novel):
        """
        功能描述：从前缀树查找模板，并返回结果
        """
        tag, template, parameter = self.trie.find(message)

        # 移除前缀树里冗余模板
        while tag != -1 and not self.id_to_dwords.get(tag):
            self.trie.delete(template)
            tag, template, parameter = self.trie.find(message)

        # 前缀树未找到模板
        if tag == -1:
            # 作为新增模板处理
            template = ''.join(message)
            if not novel_increment_dict.get(template):
                novel_increment_dict[template] = (self.template_max_id + num_novel + 1, message)
                num_novel += 1
            tag = novel_increment_dict.get(template, (-1,))[0]
            count = 0
        # 前缀树找到模板
        else:
            template = ''.join(self.templates_dict[tag]['template'])
            count = self.templates_dict[tag]['count']

        return tag, template, novel_increment_dict, num_novel, count, parameter

    def update_template(self, new_template, dict_cluster):
        for log in dict_cluster:
            tag, _, _ = self.trie.find(log['message'])
            # 模板前缀树无匹配, 需要进行更新
            if tag == -1:
                new_template = _get_template(new_template, log['message'], self.config.get('wildcard'))
        return new_template

    def update(self, log_messages_new):
        """
        功能描述：更新日志模板挖掘模型
        参数：
            log_messages：日志内容列表
        """
        log_messages_proc = preprocess_noise(log_messages_new,
                                             self.config.get('noise_masking_instructions'),
                                             self.config.get('case_sensitive'))
        log_messages_proc = tokenize(log_messages_proc,
                                     self.config.get('max_token_size'),
                                     self.config.get('word_strip'),
                                     self.config.get('delimiters'),
                                     self.config.get('wildcard'))
        dictionarize_clusters = self.group_by_dict(log_messages_proc)
        for _, dict_cluster in dictionarize_clusters.items():
            frozen_dwords = tuple(dict_cluster[0]['dwords'])
            search_key = self.dwords_to_id.get(frozen_dwords)
            if search_key is not None:  # 如果存在匹配合法词
                self.templates_dict[search_key]['count'] += len(dict_cluster)
                if self.templates_dict[search_key]['simplest']:
                    continue
                # 模板不是最简化，需要更新
                new_template = self.templates_dict[search_key]['template']
                new_template = self.update_template(new_template, dict_cluster)
                if new_template != self.templates_dict[search_key]['template']:
                    self.trie.delete(self.templates_dict[search_key]['template'])
                    self.trie.insert(new_template, search_key)
                    self.templates_dict[search_key]['template'] = new_template
                    self.templates_dict[search_key]['simplest'] = _check_template_simplest(new_template,
                                                                                           self.config.get('wildcard'),
                                                                                           self.dictionary)
            # 如果不存在匹配合法词, 先建立该合法词索引
            else:
                log0 = dict_cluster[0]
                tag0, template0, _ = self.trie.find(log0['message'])
                # 移除前缀树里冗余模板
                while tag0 != -1 and self.id_to_dwords.get(tag0) is None:
                    self.trie.delete(template0)
                    tag0, template0, _ = self.trie.find(log0['message'])

                if tag0 != -1:
                    # 模板前缀树找到, 更新新的dwords到已有模板的映射
                    self.dwords_to_id[frozen_dwords] = tag0
                    self.id_to_dwords[tag0].extend([frozen_dwords])
                else:
                    # 模板前缀树找不到，本身即为日志模板，新增模板ID，建立新的dwords到模板的映射，更新模板前缀树
                    self.template_max_id += 1
                    new_key = self.template_max_id
                    self.dwords_to_id[frozen_dwords] = new_key
                    self.id_to_dwords[new_key] = [frozen_dwords]
                    self.templates_dict[new_key] = {'template': log0['message'], 'count': 0,
                                                    'simplest': _check_template_simplest(log0['message'],
                                                                                         self.config.get('wildcard'),
                                                                                         self.dictionary)}
                    self.trie.insert(log0['message'], new_key)
                search_key = self.dwords_to_id[frozen_dwords]
                self.templates_dict[search_key]['count'] += len(dict_cluster)
                # 再进行dwords聚类下其他日志行的更新
                if self.templates_dict[search_key]['simplest']:
                    continue
                # 模板不是最简化，需要更新
                new_template = self.templates_dict[search_key]['template']
                new_template = self.update_template(new_template, dict_cluster[1:])
                if new_template != self.templates_dict[search_key]['template']:
                    self.templates_dict[search_key]['template'] = new_template
                    self.templates_dict[search_key]['simplest'] = _check_template_simplest(new_template,
                                                                                           self.config.get('wildcard'),
                                                                                           self.dictionary)
                    self.trie.insert(new_template, search_key)
