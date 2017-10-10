#-- coding: utf-8 --
import time

import math
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv
import string
import json


class SimilarUsersRatings(MRJob):

    def mapper_user_data(self, _, line):
        obj = json.loads(line)
        line = obj
        user_id = line['user_id']
        business_id = line['business_id']
        rating = line['stars']/5
        yield business_id, [user_id, rating]

    def reducer_user_pairs(self, business_id, user_ratings):
        user_rating_list = [ur for ur in user_ratings]
        #review_count = len(user_rating_list)

        for combination in itertools.combinations(user_rating_list,2):
            yield business_id, sorted(combination)

    def reducer_pair_ratings(self, business, user_count_pairs):
        #user_count_list = [u for u in user_count]

        for pair in user_count_pairs:
          user_a = pair[0][0]
          user_b = pair[1][0]
          rating_a = pair[0][1]
          rating_b = pair[1][1]
          users = sorted([user_a, user_b])
          ratings = [rating_a, rating_b]
          yield users, ratings

    def reducer_similarity(self, user_pair, rating_pairs):

        total_dividend = 0
        total_divider_a = 0
        total_divider_b = 0
        for pair in rating_pairs:
            rating_a = pair[0]
            rating_b = pair[1]
            total_dividend += (rating_a*rating_b)
            total_divider_a += (rating_a*rating_a)
            total_divider_b += (rating_b*rating_b)
        similarity = total_dividend * 1.0/(math.sqrt(total_divider_a)*math.sqrt(total_divider_b))
        if similarity > 0.8:
            yield "Pair", user_pair

    def steps(self):
        return [MRStep(mapper=self.mapper_user_data),
                MRStep(reducer=self.reducer_user_pairs),
                MRStep(reducer=self.reducer_pair_ratings),
                MRStep(reducer=self.reducer_similarity)]


if __name__ == '__main__':
    start = time.time()
    SimilarUsersRatings.run()
    end = time.time()
    print("Time: " + str(end - start) + "sec")