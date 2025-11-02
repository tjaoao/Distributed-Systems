#!/usr/bin/env python
# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# 正则表达式用于分割单词
WORD_RE = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):
    """WordCount词频统计MapReduce作业"""
    
    def mapper(self, _, line):
        """Mapper函数：将每行文本分割成单词并输出(单词, 1)"""
        # 遍历每行中的所有单词
        for word in WORD_RE.findall(line):
            # 输出小写形式的单词和计数1
            yield word.lower(), 1

    def combiner(self, word, counts):
        """Combiner函数：在Map端进行局部聚合减少数据传输量"""
        yield word, sum(counts)

    def reducer(self, word, counts):
        """Reducer函数：对相同单词的所有计数进行求和"""
        yield word, sum(counts)


class MRMultiStepWordCount(MRJob):
    """多步骤WordCount：词频统计 + 排序"""
    
    def mapper_get_words(self, _, line):
        """第一步Mapper：提取单词"""
        words = re.findall(r'\b\w+\b', line.lower())
        for word in words:
            if len(word) > 2:  # 过滤短单词
                yield word, 1
    
    def combiner_count_words(self, word, counts):
        """第一步Combiner：局部聚合"""
        yield word, sum(counts)
    
    def reducer_count_words(self, word, counts):
        """第一步Reducer：计算词频"""
        total = sum(counts)
        # 为了排序，我们输出(count, word)
        yield None, (total, word)
    
    def reducer_sort_words(self, _, count_word_pairs):
        """第二步Reducer：按词频排序"""
        # 按词频降序排序
        sorted_words = sorted(count_word_pairs, reverse=True)
        
        for count, word in sorted_words:
            yield word, count
    
    def steps(self):
        """定义两步流程"""
        return [
            MRStep(mapper=self.mapper_get_words,
                  combiner=self.combiner_count_words,
                  reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_sort_words)
        ]

if __name__ == '__main__':
    MRMultiStepWordCount.run()