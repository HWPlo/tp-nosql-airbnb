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
pd.DataFrame(list(db.airbnb.aggregate( [{"$group":{"_id": "$address.market","Prix Moyen": { "$avg": "$price" }}},{"$sort":{"Prix Moyen":-1}}])))

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
print(pd.DataFrame(list(db.airbnb.find({"address.market": "Porto"},{ "_id": 0,"name":1,"price":1} ))))

#exemples de logements à Porto Rico : 
#                               name   price
#0                Ribeira Charming Duplex   80.00
#1                      Be Happy in Porto   30.00
#7 Quels sont les 5 hôtes les plus populaires (ceux dont les propriétés ont reçu le plus de commentaires) ?
print(pd.DataFrame(list(db.airbnb.aggregate([{"$unwind":"$reviews"},{"$group":{"_id":"$_id","reviews":{"$sum":1}}},{"$sort":{"reviews":-1}},{"$limit":5}]))))
#TOP 5 des hotes les plus populaires :
#         _id  reviews
#0   4069429      533
#1  12954762      469
#2     95560      463
#3    476983      420
#4   5283892      408

#8 Quelles sont les 6 villes ayant le plus de logements disponibles à la location ?
print(pd.DataFrame(list(db.airbnb.aggregate([{"$sortByCount":"$address.market"},{"$limit":6}]))))
#Liste des villes ayant le plus de disponibilité :
#          _id  count
#0   Istanbul    660
#1   Montreal    648
#2  Barcelona    632
#3  Hong Kong    619
#4     Sydney    609
#5   New York    607

# 9 Combien de propriétés acceptent plus de 4 invités et ont une caution de moins de 300€ ?


print(pd.DataFrame(list(db.airbnb.aggregate([{"$match":{"accommodates":{"$gt":4},"security_deposit":{"$lt":300}}}]))))

#il existe 451 propriétés qui accceptent plus de 4 invités et ont une caution de moins de 300 euros 
#"[451 rows x 19 columns]"


#10 Donner les 20 utilisateurs qui ont fait le plus de commentaires (afficher seulement l'ID et le nom de l'utilisateur).

print(pd.DataFrame(list(db.airbnb.aggregate([{"$unwind":"$reviews"},{"$group":{"_id":"$reviews.reviewer_id","nom utilisateur":{"$first":"$reviews.reviewer_name"},"commentaire":{"$sum":1}}},{"$sort":{"commentaire":-1}},{"$limit":20},{"$project":{"_id":1,"nom utilisateur":1}}]))))
#Le premier utilisateur avec beaucoup de commentaires, c'est Filipe. 
#La liste:
#          _id nom utilisateur
#0    20775242          Filipe
#1    67084875            Nick
#2     2961855             Uge
#3   162027327           Thien
#4    20991911            Lisa
#5     1705870           David
#6    60816198            Todd
#7    12679057            Jodi
#8    55241576        Courtney
#9    69140895            Lisa
#10   78093968           David
#11   47303133           Lance
#12   57325457            Mary
#13   24667379           Karen
#14   86665925           Chris
#15   25715809           Megan
#16   73708321         Gonzalo
#17  128210181         Branden
#18   61469899            Erik
#19   34005800             Dan
#11 Parmi les logements à Sydney, quel est la note moyenne des visiteurs ?

print(pd.DataFrame(list(db.airbnb.aggregate([{"$match":{"address.market": "Sydney"}},{"$group":{"_id":"$address.market","note moyenne":{"$avg":"$review_scores.review_scores_rating"}}}]))))
#La note moyenne des visiteurs est de  93.45567 à Sydney
#12 Afficher les logements qui contiennent le mot "park" dans leur nom

print(pd.DataFrame(list(db.airbnb.find({"name": { "$regex": "Park","$options":"i" } },{ "name": 1, "address.street": 1 }))))

# Voici les logements qui contiennent le mot park dans leur nom, exemple Sydney Hyde Park City Apartment : 
#           _id                                               name                                      address
#0    10108388  Sydney Hyde Park City Apartment (checkin from ...   {'street': 'Darlinghurst, NSW, Australia'}
#1      102995                   UWS Brownstone Near Central Park    {'street': 'New York, NY, United States'}
#2     1070322                   Triple room Barcelona Guell park    {'street': 'Barcelona, Catalonia, Spain'}
#3    10558807                  Park Guell apartment with terrace    {'street': 'Barcelona, Catalunya, Spain'}
#4    10948197  aOceanside Hawaii Apartment Studio Kitchen Par...    {'street': 'Honolulu, HI, United States'}






