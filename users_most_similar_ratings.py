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


class SimilarUsers(MRJob):

    def mapper_user_ids(self, _, line):
        obj = json.loads(line)
        line = obj
        user_id = line['user_id']
        business_id = line['business_id']
        yield user_id, business_id

    def reducer_reviews_per_user(self, user_id, business_id):
        business_list = [br for br in business_id]
        review_count = len(business_list)

        for business_id in business_list:
            yield business_id, tuple([user_id, review_count])

    def reducer_similarity(self, business_id, user_count):
        user_count_list = [u for u in user_count]
        for combination in itertools.combinations(user_count_list,2): #each pair of users
            yield combination, 1

    def reducer_max_words_used_once(self, user_pair, value):
        pair_list = [p for p in user_pair]
        jaccard = sum(value) * 1.0 / ((pair_list[0][1] + pair_list[1][1]) - sum(value))
        yield [pair_list[0][0], pair_list[1][0]], jaccard # sum(values) * 1.0 / (key[0][1] + key[1][1] - sum(values))

    def reducer_max(self, key, values):
        similarity = sum(values)
        if similarity >= 0.5:
            yield "Pair:", [key[0],key[1]]

    def steps(self):
        return [MRStep(mapper=self.mapper_user_ids), MRStep(reducer=self.reducer_reviews_per_user),
                MRStep(reducer=self.reducer_similarity),
                MRStep(reducer=self.reducer_max_words_used_once),
                MRStep(reducer=self.reducer_max)]


if __name__ == '__main__':
    start = time.time()
    SimilarUsers.run()
    end = time.time()
    print("Time: " + str(end - start) + "sec")