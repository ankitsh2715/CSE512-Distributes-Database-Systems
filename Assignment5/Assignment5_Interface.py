#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import re
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    #query to find all business for cityToSearch
    business = collection.find({"city": {"$regex":'^'+cityToSearch+'$',"$options" :'i'}})
    #open saveLocation1 file and write all needed document fields to it
    file = open(saveLocation1, 'w')
    for b in business:
        #fields separated by $
        b_line = (b['name']).upper() + '$' + (b['full_address']).upper() + '$' + (b['city']).upper() + '$' + (b['state']).upper() + "\n"
        file.writelines(b_line)
    file.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    #query to find all business for cityToSearch
    documentList = collection.find({"categories":{'$in': categoriesToSearch}})
    
    file = open(saveLocation2, 'w')
    #iterate thru all rows
    for doc in documentList:
        # get distance between myLocation(lat,long) and business(lat,long)
        latitude = float(doc.get('latitude')) 
        longitude = float(doc.get('longitude')) 
        distance = HaversineDistance(float(myLocation[0]), float(myLocation[1]), latitude, longitude)
        #if distance is less than maxDistance, write the name of that business in saveLocation2 file
        if distance <= maxDistance:
            name = doc.get('name')
            file.write(name.upper() + '\n')
    file.close()

def HaversineDistance(lat1, lon1, lat2, lon2):
    #haversine formula to calc shortest distance between two points
    phi_lat1 = math.radians(lat1)
    phi_lat2 = math.radians(lat2)
    phi_delta = math.radians(lat2-lat1)
    lambda_delta = math.radians(lon2-lon1)
    
    a = math.sin(phi_delta/2) * math.sin(phi_delta/2) + math.cos(phi_lat1) * math.cos(phi_lat2) * math.sin(lambda_delta/2) * math.sin(lambda_delta/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    distance = 3959 * c

    return distance