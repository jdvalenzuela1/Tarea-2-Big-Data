# -*- coding: utf-8 -*-
import json

total = 768

business = open("business.json", "r")
new_business = open("new_business.json", "w")

checkin = open("checkin.json", "r")
new_checkin = open("new_checkin.json", "w")

photos = open("photos.json", "r")
new_photos = open("new_photos.json", "w")

review = open("review.json", "r")
new_review = open("new_review.json", "w")

tip = open("tip.json", "r")
new_tip = open("new_tip.json", "w")

user = open("user.json", "r")
new_user = open("new_user.json", "w")

for i in range(total):
    line = business.readline()
    new_business.write(line)
    
    line = checkin.readline()
    new_checkin.write(line)
    
    line = photos.readline()
    new_photos.write(line)
    
    line = review.readline()
    new_review.write(line)
    
    line = tip.readline()
    new_tip.write(line)
    
    line = user.readline()
    new_user.write(line)

business.close()
new_business.close()

checkin.close()
new_checkin.close()

photos.close()
new_photos.close()

review.close()
new_review.close()

tip.close()
new_tip.close()

user.close()
new_user.close()
