#-- coding: utf-8 --
import time
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv
import string
import json

regex = re.compile(r'\W+')

class UniqueReview(MRJob):

    def mapper_text_by_word(self, _, line):
        obj = json.loads(line)
        line = obj
        current_text = line['text']
        current_text = regex.sub(" ", current_text)
        words = current_text.split()
        #print(current_text)
        if len(words) < 1000:
            for word in words:
                yield word, line['text']

    def reducer_uniques_in_text(self, words, associated_text):

        for word in words:
            associated_text_list = [t for t in associated_text]
            n_associated_text_list = len(associated_text_list)
            if n_associated_text_list == 1:
                yield associated_text_list[0], 1

    def reducer_sum_uniques_in_text(self, associated_text, uniques):
        yield "Unique", [associated_text, sum(uniques)]

    def reducer_max_words_used_once(self, uniques, all_info):
        text = ""
        biggest_sum = 0

        for info in all_info:
            if info[1] > biggest_sum:
                biggest_sum = info[1]
                text = info[0]
        yield biggest_sum, text

    def steps(self):
        return [MRStep(mapper=self.mapper_text_by_word),
                MRStep(reducer=self.reducer_uniques_in_text),
                MRStep(reducer=self.reducer_sum_uniques_in_text),
                MRStep(reducer=self.reducer_max_words_used_once)]


if __name__ == '__main__':
    start = time.time()
    UniqueReview.run()
    end = time.time()
    print("Time: " + str(end - start) + "sec")