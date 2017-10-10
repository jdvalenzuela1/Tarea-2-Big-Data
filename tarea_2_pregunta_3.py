# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import re
import itertools
import csv
import string
import json
import time

# Business primero y luego Review

class UsuariosSimilares(MRJob):

    SORT_VALUES = True
      
    def mapper_join(self, _, line):    
        line = obj = json.loads(line)
        if len(line.keys()) == 15: # Business
          business_id = line["business_id"]
          categories = line["categories"]
          yield business_id, ['A', categories]
        elif len(line.keys()) == 9: # Review
          user_id = line["user_id"]
          useful = line["useful"]
          business_id = line["business_id"]
          yield business_id, [user_id, useful]
      
    def reducer_usuarios(self, keys, values):
        values = [x for x in values]
        categories = []
        for value in range(len(values)-1):
            if values[value][0] == 'A':
                categories =  values[value][1]
                values.pop(value)
        if len(categories) > 0:
            for user in values:
                for category in categories:
                    yield category, user
        else:
            pass
    # categoria, [id_usuario, cantidad de reviews en dicha categoria, el useful acumulado]
    def reducer_ponderaciones(self, category, users):
        users = [x for x in users]
        dicc = {}
        for user in users:
            user_id = user[0]
            useful = user[1]
            if user_id not in dicc.keys():
                dicc[user_id] = [1, useful]
            else:
                dicc[user_id][0] += 1
                dicc[user_id][1] += useful
        """        
        lista = []
        max_user_reviews = 0
        for user in dicc.keys():
            if dicc[user][0] > max_user_reviews:
                max_user_reviews = dicc[user][0]
                user_useful = dicc[user][1]
                lista = [[user, "Total Reviews: " + str(max_user_reviews), "Ponderado: " + str(user_useful/max_user_reviews*1.0)]]
            elif dicc[user][0] == max_user_reviews:
                user_useful = dicc[user][1]
                lista.append([user, "Total Reviews: " + str(max_user_reviews), "Ponderado: " + str(user_useful/dicc[user][0]*1.0)])
	"""
	mejor_user = 0
	max_user_reviews = 0
	user_useful = 0
	for user in dicc.keys():
		if dicc[user][0] >= max_user_reviews:
			max_user_reviews = dicc[user][0]
			user_useful = dicc[user][1]
			mejor_user = user
	yield category, [mejor_user, "Total Reviews: " + str(max_user_reviews), "Ponderado: " + str(user_useful/dicc[user][0]*1.0)]
        # yield category, lista
        
    def steps(self):
        return [MRStep(mapper=self.mapper_join), MRStep(reducer=self.reducer_usuarios),
                MRStep(reducer=self.reducer_ponderaciones)]


if __name__ == '__main__':
	start_time = time.time()
	UsuariosSimilares.run()
	print "--- %s seconds ---" % (time.time() - start_time)
    
