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


class ReviewUnico(MRJob):
    def mapper_textos_por_palabra(self, _, line):
        line = obj = json.loads(line)
        texto_actual = line['text']
        texto_actual = re.sub(r"[,.;@#?!&$]+\ *", " ", texto_actual)
        palabras = texto_actual.split() 
        for palabra in palabras:
            yield palabra.encode('utf-8'), line["text"]

    def reducer_unicos_por_texto(self, palabras, textos_asociados):
        textos_asociados_list = [t for t in textos_asociados]
        n_textos_asociados_list = len(textos_asociados_list)
        if n_textos_asociados_list == 1:
            yield textos_asociados_list[0], 1
                
    def reducer_suma_unicos_por_texto(self, texto_asociado, unicos):
        yield "Unico", [texto_asociado, sum(unicos)]

    def reducer_maximo_palabras_usadas_una_vez(self, unico, todo_info):
        texto = ""
        suma_mayor = 0
        for info in todo_info:
            if info[1] > suma_mayor:
                suma_mayor = info[1]
                texto = info[0]
        yield suma_mayor, texto

    def steps(self):
        return [MRStep(mapper=self.mapper_textos_por_palabra), MRStep(reducer=self.reducer_unicos_por_texto),
                MRStep(reducer=self.reducer_suma_unicos_por_texto), MRStep(reducer=self.reducer_maximo_palabras_usadas_una_vez)]


if __name__ == '__main__':
	start_time = time.time()
	ReviewUnico.run()
	print "--- %s seconds ---" % (time.time() - start_time)
