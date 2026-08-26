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

import re
import string

from ..models import wordninja


def preprocess_noise(log_messages, noise_masking_instructions, case_sensitive):
    """
    功能描述：根据预设正则表达式noise_masking_instructions，将噪声字符串替换为通配符
    参数：
        log_messages：日志内容列表
    返回值：
        预处理后的日志内容列表
    """
    preprocessed_log_messages = [None] * len(log_messages)
    # priori knowledge preprocess
    for index in range(len(log_messages)):
        value = log_messages[index]
        for mi in noise_masking_instructions:
            value = re.sub(mi.get('regex_pattern', ''), mi.get('mask_with', ''), value)
        # 中文与非中文之间添加空格
        value = re.sub(r'([\u4e00-\u9fff]+)', r' \1 ', value)
        # 连续空格符只保留一个
        value = re.sub(r' +', r' ', value)
        if not case_sensitive:
            value = value.lower()
        preprocessed_log_messages[index] = value
    return preprocessed_log_messages


def _process_delimiters(value, word_strip, delimiters, max_token_size):
    """
    功能描述：对分隔符进行处理
    """
    left = 0
    tokens = []
    for right, _ in enumerate(value):
        if value[right] in delimiters:
            token_new = value[left:right]
            token_new_strip = token_new.strip(word_strip)
            # 防止连续格式符导致出现空值
            if token_new_strip:
                tokens.append(token_new_strip)
            elif token_new:
                tokens.append(token_new)
            tokens.append(value[right])
            left = right + 1
            # 超过了最大分词数量
            if len(tokens) > max_token_size:
                break
    return tokens, left


def tokenize(log_messages, max_token_size, word_strip, delimiters, wildcard):
    """
    功能描述：根据预设分隔符，将每一条日志内容分解为单词列表（日志分词向量）, 并且若分词是字母+数目的复合词，将进一步拆分成字母部分和数字部分
    参数：
        log_messages：日志内容列表
    返回值：
        分词处理后的日志分词向量列表
    """
    tokenized_log_messages = [None] * len(log_messages)
    escape_wildcard = re.escape(wildcard)

    for index in range(len(log_messages)):
        value = log_messages[index].strip()
        value_list = re.split('({})'.format(escape_wildcard), value)
        tokens = []
        max_token_size_remain = max_token_size

        # 对分隔符进行处理
        for value_sub in value_list:
            if value_sub == wildcard:
                tokens.append(value_sub)
                max_token_size_remain -= 1
            else:
                tokens_sub, left = _process_delimiters(value_sub, word_strip, delimiters, max_token_size_remain)
                token_new = value_sub[left:]
                token_new_strip = token_new.strip(word_strip)
                # 防止连续格式符导致出现空值
                if token_new_strip:
                    tokens_sub.append(token_new_strip)
                elif token_new:
                    tokens_sub.append(token_new)
                max_token_size_remain -= len(tokens_sub)
                tokens.extend(tokens_sub)
        tokenized_log_messages[index] = tokens

    return tokenized_log_messages


def _is_punctuation(word):
    """
    功能描述：判断单词是否是标点符号
    """
    return all(char in string.punctuation + ' ' for char in word)


def _check_chinese(word, force_chinese_valid):
    """
    功能描述：是否包含合法中文字符
    """
    return force_chinese_valid and re.search(u'[\u4e00-\u9fff]', word)


def check_valid_list(word_list, dictionary, config):
    """
    功能描述：判定一个列表中的日志分词是否有合法词
    """
    return any(check_valid(word, dictionary, config) for word in word_list)


def check_valid(word, dictionary, config):
    """
    功能描述：判定日志分词是否是合法词
    参数：
        word：单个日志分词
        trie_config: 前缀树匹配参数
    """
    min_word_length = config.get('min_word_length')
    max_word_length = config.get('max_word_length')
    enable_compose_valid_check = config.get('enable_compose_valid_check')
    compose_delimiters = config.get('compose_delimiters')
    min_compose_word_length = config.get('min_compose_word_length')
    force_chinese_valid = config.get('force_chinese_valid')

    if not (min_word_length <= len(word) <= max_word_length):
        return False
    elif _check_chinese(word, force_chinese_valid) or word in dictionary:
        return True
    # 判断纯字母+复合词连接词是否是合法词
    elif enable_compose_valid_check:
        compose_type = _get_compose_type(word, compose_delimiters)
        if compose_type == 1:
            splitted_words = wordninja.split(word)
            for sword in splitted_words:
                if (len(sword) >= min_compose_word_length) and (sword in dictionary):
                    return True
    return False


def _get_compose_type(word, compose_delimiters):
    """
    功能描述：判断给定单词的复合词类型
    参数：
        word：单词
        compose_delimiters：复合词的连接词列表
    返回值：
        0: 非复合词
        1：纯字母+复合词连接词
        2：字母+数字+复合词连接词
    """
    compose_type = 0
    if any(char.isalpha() for char in word):
        if all(char.isalpha() or char in compose_delimiters for char in word):
            compose_type = 1
        elif all(char.isalnum() or char in compose_delimiters for char in word):
            compose_type = 2
    return compose_type


def _preprocess_dictionary(dictionary, words_add, words_drop):
    """
    功能描述：根据业务提供的合法词新增与过滤列表更新合法词字典
    参数：
        dictionary：合法词字典
        words_add：业务自定义日志合法词列表
        words_drop：业务自定义日志过滤词列表
    返回值：
        更新后的合法词字典
    """
    for word in words_add:
        if word not in dictionary:
            dictionary.add(word)

    for word in words_drop:
        if word in dictionary:
            dictionary.remove(word)

    return dictionary


def _maskdel_wildcard(template, wildcard):
    """
    功能描述：对通配符进行掩码
    """
    temp = []
    for token in template:
        if token == wildcard:
            temp.append('')
        else:
            temp.append(token)
    return temp


def _check_template_simplest(template, wildcard, dictionary):
    """
    功能描述：检查模板是不是已经最简化
    """
    simplest = True
    for index in range(1, len(template)):
        word = template[index]
        if word == wildcard or _is_punctuation(word):
            continue
        if word.lower() not in dictionary:
            return False
    return simplest


def _get_template(seq_base, seq_other, wildcard):
    """
    功能描述：获取模板
    """
    template = [wildcard] * len(seq_base)
    for i, token in enumerate(seq_base):
        if seq_other[i] == token:
            template[i] = token
    return template
