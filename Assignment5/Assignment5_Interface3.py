#
# Assignment5 Interface
# Name: Shubhangi Gupta
#

from pymongo import MongoClient
import os
import sys
import json
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):

    #find all businesses for cityToSearch
    businessCollection = collection.find({"city": {"$regex":'^'+cityToSearch+'$',"$options" :'i'}})

    file = open(saveLocation1, 'w')

    #iterate through business collection 
    for business in businessCollection:
        business_row = (business['name']).upper() + '$' + (business['full_address']).upper() + '$' + (business['city']).upper() + '$' + (business['state']).upper() + "\n"
      
        #field values separated by $ and written in uppercase in saveLocation1 file
        file.writelines(business_row)
    file.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
   
    #find all businesses for categoriesToSearch
    businessCollection = collection.find({"categories":{'$in': categoriesToSearch}})
    
    file = open(saveLocation2, 'w')

    for business in businessCollection:

        # get distance between myLocation and business
        latitude = float(business.get('latitude')) 
        longitude = float(business.get('longitude')) 
        distance = CalculateDistance(float(myLocation[0]), float(myLocation[1]), latitude, longitude)

        #check if distance is less than equal to maxDistance and write the name of that business in upper case in saveLocation2 file 
        if distance <= maxDistance:
            name = business.get('name')
            file.write(name.upper() + '\n')
    file.close()

#Haversine formula to calculate distance between two pairs of latitude and longitude
def CalculateDistance(latitude1, longitude1, latitude2, longitude2):
    
    r= 3959
    phi_latitude1 = math.radians(latitude1)
    phi_latitude2 = math.radians(latitude2)
    phi_delta = math.radians(latitude2-latitude1)
    lambda_delta = math.radians(longitude2-longitude1)
    
    a = math.sin(phi_delta/2) * math.sin(phi_delta/2) + math.cos(phi_latitude1) * math.cos(phi_latitude2) * math.sin(lambda_delta/2) * math.sin(lambda_delta/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    distance = r * c

    return distance

