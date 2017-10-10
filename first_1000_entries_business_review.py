# -*- coding: utf-8 -*-
import json

total = 100

business = open("business.json", "r")
new_business = open("new_business.json", "w")

review = open("review.json", "r")
new_review = open("new_review.json", "w")

business_ids = []

for i in range(total): # En review.json
    line = review.readline()
    new_review.write(line)
    line = obj = json.loads(line)
    business_ids.append(line['business_id'])


conteo = 0
for i in business: #En business.json
    json_line = obj = json.loads(i)
    if json_line['business_id'] in business_ids:
        new_business.write(i)
        conteo += 1
    if conteo == total:
        break
    print conteo

business.close()
new_business.close()

review.close()
new_review.close()
