# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv
import string
import json
import math
import time

class MetricaSimilaridad(MRJob):
      
    def mapper_user_business(self, _, line):
        line = obj = json.loads(line)
        user = line['user_id']
        business_id = line['business_id']
        stars = line['stars']
        yield user, [business_id, stars]

    # id_business, [id_usuario, estrellas normalizadas]
    def reducer_estrellas(self, user, business_info):
        business_info = [x for x in business_info]
        dicc = {}
        for business in business_info:
            if business[0] not in dicc.keys():
                dicc[business[0]] = int(business[1])/5.0
            else:
                dicc[business[0]] += int(business[1])/5.0
        for business in dicc.keys():
            yield business, [user, dicc[business]]

    def reducer_combination(self, business_id, user_and_stars):
        for combination in itertools.combinations(user_and_stars, 2):
            sorted_combination = sorted(combination)
            id_a = sorted_combination[0][0]
            id_b = sorted_combination[1][0]
            estrellas_normalizadas_a = sorted_combination[0][1]
            estrellas_normalizadas_b = sorted_combination[1][1]
            yield [id_a, id_b], [estrellas_normalizadas_a, estrellas_normalizadas_b, business_id]

    def reducer_ponderacion(self, combination, info):
        estrellas = [x for x in info]
        total_estrellas_a = 0
        total_estrellas_b = 0
        total_ai_por_bi = 0
        for value in estrellas:
            total_estrellas_a += value[0]
            total_estrellas_b += value[1]
            total_ai_por_bi += value[0]*value[1]
        yield 'MAX', [combination, (total_ai_por_bi)/((math.sqrt(total_estrellas_a**2))*(math.sqrt(total_estrellas_b**2)))]

    def reducer_max(self, key, values):
            tupla = ""
            suma_mayor = 0
            for value in values:
                if value[1] > suma_mayor:
                    suma_mayor = value[1]
                    tupla = value[0]              
            yield tupla, suma_mayor

    def steps(self):
        return [MRStep(mapper=self.mapper_user_business), MRStep(reducer=self.reducer_estrellas),
                MRStep(reducer=self.reducer_combination), MRStep(reducer=self.reducer_ponderacion),
                MRStep(reducer=self.reducer_max)]


if __name__ == '__main__':
	start_time = time.time()
    	MetricaSimilaridad.run()
	print "--- %s seconds ---" % (time.time() - start_time)
