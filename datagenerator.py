#!/usr/bin/env python
# coding: utf-8



# SETUP
from datetime import datetime
from faker import Faker
import io
import keyboard
import numpy as np
import pickle
import random
import time




faker = Faker('es_ES')
USERS_TOTAL = 10000
MAX_FRIENDS = 10
users={}
lat_min = 39.4
lat_max = 39.5
lon_min = -0.3
lon_max = -0.4
vehicles = ["Bike","Train","Car", "Walking"]




def initiate_data():
    global users
    for i in range(0,USERS_TOTAL):
        user = {}
        user["id"] = faker.ssn()
        user["name"] = faker.first_name()
        user["last_name"] = faker.last_name()
        user["friends"] = []
        user["position"] = {"lat":random.uniform(39.4, 39.5),"lon":random.uniform(-0.3, -0.4)}
        user["transport"] = random.choice(vehicles)
        user["age"] = random.uniform(16, 85)
        user["gender"] = random.choice(["man","woman"])
        user["weight"] = random.uniform(60, 110)
        user["height"] = random.uniform(150, 210)
        user["bodyfat"] = random.uniform(3, 45)
        user["bloodpressure_sist"] = random.uniform(90, 180)
        user["bloodpressure_diast"] = random.uniform(70, 120)
        user["cholesterol"] = random.uniform(150, 300)
        user["smoker"] = random.choice(["0","1"])
        user["drinking"] = random.uniform(0,7)
        user["disability"] = random.choice(["0","1"])
        user["previouspatology"] = random.choice(["0","1"])
        user["cp"] = random.randint(46001, 46025)
        user["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        users[user["id"]] = user   
        # print(user)
    num=0
    for element in users.items():
        # print(f"Generating friends of {num} of {len(users)}")
        for i in range(0, random.randint(1, MAX_FRIENDS)):
            friend = random.choice(list(users.values()))
            if friend["id"] != element[0]:
                users[element[0]]["friends"].append(friend["id"])
            else:
                pass
                # print("No friend of yourself") 
        num = num + 1

    print("DATA GENERATED")




def generate_step():
    global users
    if len(users)>0:
        print("STEP")
        for element in users.items():
            users[element[0]]["time"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            lat=users[element[0]]["position"]["lat"]
            lon=users[element[0]]["position"]["lon"]
            users[element[0]]["position"]["lon"]=lon+random.uniform(0.001, 0.005)
            users[element[0]]["position"]["lat"]=lat+random.uniform(0.001, 0.005)
            if lat>lat_max or lat<lat_min:
                users[element[0]]["position"]["lat"]=random.uniform(39.4, 39.5)
                users[element[0]]["transport"]=random.choice(vehicles)
            if lon>lon_max or lon<lon_min:
                users[element[0]]["position"]["lon"]=random.uniform(-0.3, -0.4)
            
    else:
        initiate_data()
    return users




k = 0
# Iterable
iter_np = range(USERS_TOTAL)

while True:
    try:  
        if keyboard.is_pressed('q'):  # if key 'q' is pressed 
            print('You Exited the data generator')
            break  
        else:
            users_generated=generate_step()
            # The code goes here
            print("code")
            # Send the data to Kafka/database (instead of writing inside a folder)
                # Maybe, different filename has not any sense at Kafka.
            filename = 'data/data_' + str(k) + '.pkl'
            temp = np.array(list(map(lambda v: list(list(users_generated.values())[v].values()), iter_np)), dtype=object)
            temp = np.array(list(map(lambda v: np.delete(np.append(temp[v], [temp[v][4].get('lat'), temp[v][4].get('lon')]), 4), iter_np)))
            
            # FIXME: consider feather format
            with open(filename, 'wb') as f:
                pickle.dump(temp, f, protocol=pickle.HIGHEST_PROTOCOL)
            # Wait
                # Maybe, it could be removed.
            time.sleep(2)
            k += 1
            
    except Exception as err:
        print(f"Unexpected {err}, {type(err)}")
        break

