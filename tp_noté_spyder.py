# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 09:56:10 2024

@author: hsiao.lo
"""

import pymongo
import pandas as pd

URI = 'mongodb+srv://mongo_user:qkAbDeC5vFYwniqK@cluster-but-sd.z5gy4.mongodb.net/?retryWrites=true&w=majority&appName=cluster-but-sd'
client = pymongo.MongoClient(URI)
db = client.tp
print("Collections: ", db.list_collection_names())



#1 Combien de logements sont la base de données ? 
print(db.airbnb.count_documents({}))
# Il existe 5555 logements dans notre base de données 

#2 Quel est le prix moyen par ville ? Trier les villes par prix décroissant.
c= db.airbnb.aggregate( [{"$group":{"_id": "$address.market","Prix Moyen": { "$avg": "$price" }}},{"$sort":{"Prix Moyen":-1}}])
#Prix moyen de : Hong Kong 763, Rio De Janeiro  526,Other (International) 446, Istanbul  368,Kauai 289, Maui 287, Oahu 212, Sydney 198, The Big Island 179, New York 140, Other(Domestic) 128, Barcelona 101, Montreal 100, Porto 69

#3 Afficher la liste de tous les différents équipements qui existent.
print(db.airbnb.distinct(key="amenities"))
#il existe comme équipements : ['', '24-hour check-in', 'Accessible-height bed', 'Accessible-height toilet', 'Air conditioning', 'Air purifier', 'Alfresco shower', 'BBQ grill', 'Baby bath', 'Baby monitor', 'Babysitter recommendations', 'Balcony', 'Bath towel', 'Bathroom essentials', 'Bathtub', 'Bathtub with bath chair', 'Beach chairs', 'Beach essentials', 'Beach view', 'Beachfront', 'Bed linens', 'Bedroom comforts', 'Bicycle', 'Bidet', 'Body soap', 'Boogie boards', 'Breakfast', 'Breakfast bar', 'Breakfast table', 'Building staff', 'Buzzer/wireless intercom', 'Cable TV', 'Carbon monoxide detector', 'Cat(s)', 'Ceiling fan', 'Central air conditioning', 'Changing table', "Chef's kitchen", 'Children’s books and toys', 'Children’s dinnerware', 'Cleaning before checkout', 'Coffee maker', 'Convection oven', 'Cooking basics', 'Crib', 'DVD player', 'Day bed', 'Dining area', 'Disabled parking spot', 'Dishes and silverware', 'Dishwasher', 'Dog(s)', 'Doorman', 'Double oven', 'Dryer', 'EV charger', 'Electric profiling bed', 'Elevator', 'En suite bathroom', 'Espresso machine', 'Essentials', 'Ethernet connection', 'Extra pillows and blankets', 'Family/kid friendly', 'Fax machine', 'Fire extinguisher', 'Fireplace guards', 'Firm mattress', 'First aid kit', 'Fixed grab bars for shower', 'Fixed grab bars for toilet', 'Flat path to front door', 'Formal dining area', 'Free parking on premises', 'Free street parking', 'Full kitchen', 'Game console', 'Garden or backyard', 'Gas oven', 'Ground floor access', 'Gym', 'Hair dryer', 'Handheld shower head', 'Hangers', 'Heated towel rack', 'Heating', 'High chair', 'Home theater', 'Host greets you', 'Hot tub', 'Hot water', 'Hot water kettle', 'Ice Machine', 'Indoor fireplace', 'Internet', 'Iron', 'Ironing Board', 'Kayak', 'Keypad', 'Kitchen', 'Kitchenette', 'Lake access', 'Laptop friendly workspace', 'Lock on bedroom door', 'Lockbox', 'Long term stays allowed', 'Luggage dropoff allowed', 'Memory foam mattress', 'Microwave', 'Mini fridge', 'Mountain view', 'Murphy bed', 'Netflix', 'Other', 'Other pet(s)', 'Outdoor parking', 'Outdoor seating', 'Outlet covers', 'Oven', 'Pack ’n Play/travel crib', 'Paid parking off premises', 'Paid parking on premises', 'Parking', 'Patio or balcony', 'Permit parking', 'Pets allowed', 'Pets live on this property', 'Pillow-top mattress', 'Pocket wifi', 'Pool', 'Pool with pool hoist', 'Private bathroom', 'Private entrance', 'Private hot tub', 'Private living room', 'Private pool', 'Rain shower', 'Refrigerator', 'Roll-in shower', 'Room-darkening shades', 'Safe', 'Safety card', 'Sauna', 'Self check-in', 'Shampoo', 'Shared pool', 'Shower chair', 'Single level home', 'Ski-in/Ski-out', 'Smart TV', 'Smart lock', 'Smoke detector', 'Smoking allowed', 'Snorkeling equipment', 'Sonos sound system', 'Sound system', 'Stair gates', 'Standing valet', 'Step-free access', 'Stove', 'Suitable for events', 'Sun loungers', 'Swimming pool', 'TV', 'Table corner guards', 'Tennis court', 'Terrace', 'Toaster', 'Toilet paper', 'Walk-in shower', 'Warming drawer', 'Washer', 'Washer / Dryer', 'Waterfront', 'Well-lit path to entrance', 'Wheelchair accessible', 'Wide clearance to bed', 'Wide clearance to shower', 'Wide doorway', 'Wide entryway', 'Wide hallway clearance', 'Wifi', 'Window guards', 'toilet',

#4 Combien de propriétés incluent le Wifi dans les équipements ?
print(db.airbnb.count_documents({"amenities":"Wifi"}))
# Il existe 5303 logements qui ont le wifi.

#5 Afficher le nom de tous les logements ainsi que le nombre de chambres et de lits qu'ils contiennent (ne pas afficher l'ID)
print(pd.DataFrame(list(db.airbnb.find({},{ "_id": 0, "name":1,"bedrooms": 1,"beds":1} ))))
# Exemples issus de la requete :                                                   name  bedrooms  beds
#0                          Apt Linda Vista Lagoa - Rio       1.0   1.0
#                   Ótimo Apto proximo Parque Olimpico       2.0   2.0

#6 Afficher le nom et le prix des logements situés à Porto.
print(list(db.airbnb.find({"name": "Porto"},{ "_id": 0,"name":1,"$price":1} )))
#print(list(db.restaurants.find({"name": "Burger King"},{ "name":1,"address.street": 1 } )))
#print(pd.DataFrame(list(c)))




c = db.restaurants.aggregate([{"$addFields":{"first_note":{"$first":"$grades.score"}}},{"$group":{"_id":"$borough","nb_restaurants":{"$sum":1},"score_moyen":{"$avg":"$first_note"}}},{"$sort":{"score_moyen":1}}])
#print(pd.DataFrame(list(c)).round(2))
