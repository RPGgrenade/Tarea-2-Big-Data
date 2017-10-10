import time
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv
import string
import json


class PopularUsersPerCategory(MRJob):

    def mapper_user_category(self, _, line):
        obj = json.loads(line)
        line = obj
        user_id = None
        categories = None
        use_vote = 0
        fun_vote = 0
        cool_vote = 0
        try:    #business line
            categories = line['categories']
        except: #review line
            user_id = line['user_id']
            use_vote = line['useful']
            fun_vote = line['funny']
            cool_vote = line['cool']
        business_id = line['business_id']
        votes = use_vote + fun_vote + cool_vote #assumption of relevance being related to all three
        if categories: #if looking at the business stuff
            symbol = 'B'
            yield business_id, [symbol, categories]
        else: #review stuff
            symbol = 'A'
            yield business_id, [symbol, user_id, votes]

    def reducer_join_business_review(self, business_id, values):
        reviews = []

        for value in values:
            if value[0] == 'A':
                reviews.append(value)
            if value[0] == 'B':
                for review in reviews:
                    full_review = review[1:] + value[1:]
                    yield business_id, full_review

    def reducer_categorize_user_votes(self, business_id, reviews):

        for review in reviews:
            user_id = review[0]
            vote_count = review[1]
            categories = review[2]
            for category in categories:
                yield category, [user_id, vote_count]

    def reducer_most_popular_user(self, category, user_vote):

        user_dict = dict()
        for item in user_vote:
            user_id = item[0]
            votes = item[1]
            if user_id in user_dict:
                user_dict[user_id].append(votes)
            else:
                user_dict[user_id] = [votes]

        most_popular_user = None
        max_popularity = 0
        for user, vote_list in user_dict.items():
            review_count = len(vote_list)
            review_popularity = sum(vote_list)
            # assumption of count being more important, with popularity average adding onto it
            # a single extremely useful or popular review is similar to many irrelevant reviews
            popularity = review_count + (review_popularity/review_count)
            if popularity > max_popularity:
                max_popularity = popularity
                most_popular_user = user

        yield category, most_popular_user

    def steps(self):
        return [MRStep(mapper=self.mapper_user_category),
                MRStep(reducer=self.reducer_join_business_review),
                MRStep(reducer=self.reducer_categorize_user_votes),
                MRStep(reducer=self.reducer_most_popular_user)]


if __name__ == '__main__':
    start = time.time()
    PopularUsersPerCategory.run()
    end = time.time()
    print("Time: " + str(end - start) + "sec")