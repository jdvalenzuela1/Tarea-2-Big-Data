from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv
import json
import time

class SimilarUsers(MRJob):
    def mapper_user_business(self, _, line):
        line = obj = json.loads(line)
        user = line['user_id']
        business = line['business_id']        
        yield user, business

    def reducer_total_business(self, user_id, business_ids):
        business_list = [b for b in business_ids]
        n_business = len(business_list)
        for business_id in business_list:
            yield business_id, tuple([user_id, n_business])

    def reducer_combination(self, business_id, extended_userids):
        for combination in itertools.combinations(extended_userids, 2):
            sorted_combination = sorted(combination)
            yield sorted_combination, 1

    def reducer_jaccard(self, key, values):
        yield 'MAX', [sum(values)*1.0/(key[0][1] + key[1][1] - sum(values)), [key[0][0], key[1][0]]]

    def reducer_max(self, key, values):
        tupla = ""
        suma_mayor = 0
        for value in values:
            if value[0] > suma_mayor:
                suma_mayor = value[0]
                tupla = value[1]              
        yield tupla, suma_mayor

    def steps(self):
        return [MRStep(mapper=self.mapper_user_business, reducer=self.reducer_total_business),
                MRStep(reducer=self.reducer_combination), MRStep(reducer=self.reducer_jaccard),
                MRStep(reducer=self.reducer_max)]

if __name__ == '__main__':
	start_time = time.time()
	SimilarUsers.run()
	print "--- %s seconds ---" % (time.time() - start_time)
