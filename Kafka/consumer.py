from confluent_kafka import Consumer
from confluent_kafka import Producer
from avro.io import DatumReader, BinaryDecoder
import avro.schema
import io
import base64
import numpy as np
import json
import ast
# from datetime import datetime
import geopy.distance
# import glob
import keyboard
import pandas as pd
import time

USERS_TOTAL = 1000
iter_np = range(USERS_TOTAL)
MAX_FRIENDS = 10

# FUNCTIONS

# Inputs from dataframe
    # El resultado diario no puede ser mayor a 14.05 km
def get_distance_km(lat1, lat2, lon1, lon2):
    pos1 = (lat1, lon1)
    pos2 = (lat2, lon2)
    return geopy.distance.distance(pos1, pos2).km


def score_IMC(weight, height):
    IMC = weight / ((height / 100)**2)
    if IMC < 16:
        return 33
    elif IMC >= 16 and IMC < 18.5:
        return 66
    elif IMC >= 18.5 and IMC < 25:
        return 100
    elif IMC >= 25 and IMC < 30:
        return 66
    else:
        return 33


def age_bodyfat(age, gender):
    if age < 30:
        if gender == 'male':
            return [11, 14, 21, 23]
        else:
            return [16, 20, 29, 31]
    elif age >= 30 and age < 40:
        if gender == 'male':
            return [12, 15, 22, 24]
        else:
            return [17, 21, 30, 32]
    elif age >= 40 and age < 50:
        if gender == 'male':
            return [14, 17, 24, 26]
        else:
            return [18, 22, 31, 33]
    else:
        if gender == 'male':
            return [14, 17, 24, 26]
        else:
            return [19, 23, 32, 34]          


def score_bodyfat(age, gender, bodyfat):
    if bodyfat < age_bodyfat(age, gender)[0]:
        return 100
    elif bodyfat >= age_bodyfat(age, gender)[0] and bodyfat < age_bodyfat(age, gender)[1]:
        return 80
    elif bodyfat >= age_bodyfat(age, gender)[1] and bodyfat < age_bodyfat(age, gender)[2]:
        return 60
    elif bodyfat >= age_bodyfat(age, gender)[2] and bodyfat < age_bodyfat(age, gender)[3]:
        return 40
    else:
        return 20


def score_cholesterol(cholesterol):
    if cholesterol < 200:
        return 100
    elif cholesterol >= 200 and cholesterol < 240:
        return 50
    else:
        return -100


def score_drink(drinking):
    if drinking < 0.5:
        return 100
    elif drinking >= 0.5 and drinking < 2:
        return 80
    elif drinking >= 2 and drinking < 4:
        return 40
    else:
        return -100


# df_1
def score_km(km_walk, km_bike):
    # La persona andó ese día
    if km_walk > 0 and km_walk < 8:
        return km_walk*12.5
    elif km_walk >= 8:
        return 100
    # Los que han ido en bici, tren o coche
    else:
        if km_bike > 0:
            return km_bike*3.6
        else:
            return 0


# Get the score for each user
    # x: id
    # df_0: temp.loc[x]
        # 12: cholesterol; 13: smoker; 14: disability; 15: previouspatology; 18: lat; 19: lon.
    # df_1: df_real_time.loc[df_real_time.id == x]
        # 1: km_walk; 2: km_bike

# Putuación mínima posible: -11.4
# Puntuación máxima posible: 100
# La puntuación debería ser la media.
def get_score(df_0, walk, bike):
    # IMC -> weight: 7; height: 8.
    score = score_IMC(df_0[7], df_0[8])*0.2
    # Bodyfat -> age: 5; gender: 6; bodyfat: 9
    score += score_bodyfat(df_0[5], df_0[6], df_0[9])*0.1
    # Cholesterol: 12
    score += score_cholesterol(df_0[12])*0.1
    # Smoker: 13
    score += (-200*int(df_0[13]) + 100)*0.05
    # Drinking: 14
    score += score_drink(df_0[14])*0.05
    # Disability: 15
    score += (-200*int(df_0[15]) + 100)*0.05
    # Previous patology: 16
    score += (-200*int(df_0[16]) + 100)*0.05
    # km_walk: 1; km_bike: 2.
    score += score_km(walk, bike)*0.4
    score = 17.6277 + 0.823723*score
    return score

# Receive and decode bytes to numpy array
def decode_data(j_dumps:str):
    # Convert Base 64 representation to byte representation
    data = base64.b64decode(j_dumps).decode()
    return data

def encode_to_bytes(array_) -> str:
    # Convert the byte representation to base 64 representation
    data = base64.b64encode(bytes(str(array_), 'utf-8'))
    return data

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 0,
    'auto.offset.reset': 'earliest'
})

p = Producer({'bootstrap.servers': 'localhost:9092'})


# CODE 
time_count = list()
# Iterable
iter_np = range(USERS_TOTAL)
df = np.array([]).reshape(0,21)

col_names = list()
list(map(lambda x: col_names.append('friend_' + str(x)), range(1,MAX_FRIENDS+1)))
df_friends = pd.DataFrame(columns = col_names)
k = 0

## LOOP
while True:
    c.subscribe(['user_data'])
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue


    # decoded = decode(msg.value())
    # print('Received message: {}'.format(receive_and_decode_bytes_to_numpy_array(msg.value())))
    # print(json.loads(msg.value))

    start_time = time.time()
    # Get user_data
    # Decode user data
    temp = decode_data(msg.value())
    temp = ast.literal_eval(temp)
    temp = np.array(list(map(lambda v: list(list(temp.values())[v].values()), iter_np)), dtype=object)
    temp = np.array(list(map(lambda v: np.delete(np.append(temp[v], [temp[v][4].get('lat'), temp[v][4].get('lon')]), 4), iter_np)))
    df = np.concatenate([df, temp])

    # Get user friends
    c.subscribe(['user_friends'])
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    friends = decode_data(msg.value())
    friends = ast.literal_eval(friends)

    if k > 0:
        # Add values
        # print(f'Before km_walk and bike: {time.time() - start_time}')
        rt_np_walk = np.array(list(map(lambda x: get_distance_km(df[-2*USERS_TOTAL+x][-2], 
                                                                 df[-USERS_TOTAL+x][-2], 
                                                                 df[-2*USERS_TOTAL+x][-1], 
                                                                 df[-USERS_TOTAL+x][-1]) 
                                       if df[-2*USERS_TOTAL+x][4] == 'Walking' and df[-USERS_TOTAL+x][4] == 'Walking' 
                                       else 0, 
                                       iter_np)))
        rt_np_walk_total += rt_np_walk
        
        rt_np_bike = np.array(list(map(lambda x: get_distance_km(df[-2*USERS_TOTAL+x][-2], 
                                                                 df[-USERS_TOTAL+x][-2], 
                                                                 df[-2*USERS_TOTAL+x][-1], 
                                                                 df[-USERS_TOTAL+x][-1]) 
                                       if df[-2*USERS_TOTAL+x][4] == 'Bike' and df[-USERS_TOTAL+x][4] == 'Bike' 
                                       else 0, 
                                       iter_np)))
        rt_np_bike_total += rt_np_bike

        # print(f'Before score: {time.time() - start_time}')
        # First score registered
        if k == 1:
            rt_np_score = np.array(list(map(lambda x: get_score(temp[x], rt_np_walk[x], rt_np_bike[x]), 
                                            iter_np)))

        else:
            rt_np_score = np.array(list(map(lambda x: np.mean([get_score(temp[x], rt_np_walk[x], rt_np_bike[x]),
                                                               rt_np_score[x]]), 
                                            iter_np)))

        
    # First iteration
    else:
        temp_id = np.array(list(map(lambda x: x[0], temp)))
        rt_np_walk = np.zeros(temp_id.shape)
        rt_np_bike = np.zeros(temp_id.shape)
        rt_np_score = np.zeros(temp_id.shape)
        rt_np_walk_total = np.zeros(temp_id.shape)
        rt_np_bike_total = np.zeros(temp_id.shape)
        
        # Friends dataframe
        # FIXME: with Kafka running, it will be different.
        df_friends = pd.DataFrame(list(map(lambda x: temp[x][3], iter_np)), columns = col_names)
        df_friends.index = pd.Index(temp_id)
        # Save friends dataframe


    # Create list of list for score
    user_score = list()
    user_score.append(temp_id.tolist())
    user_score.append(rt_np_walk.tolist())
    user_score.append(rt_np_bike.tolist())
    user_score.append(rt_np_score.tolist())

    # Send data to topics
    p.poll(k)
    p.produce('user_score', encode_to_bytes(list(user_score)), callback = delivery_report)
    p.flush()

    # Drop rows
    df = df[-2*USERS_TOTAL:]
    # Print execution time
    time_count.append(time.time() - start_time)
    k += 1
    # print(user_score)
    # print("--- %s seconds ---" % time_count[-1])

c.close()
