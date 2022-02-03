from confluent_kafka import Consumer
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
import folium
from folium import Choropleth, Circle, Marker
from folium.plugins import HeatMap, MarkerCluster

USERS_TOTAL = 1000
iter_np = range(USERS_TOTAL)
MAX_FRIENDS = 10

# FUNCTIONS

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
        print('[consumer_map.py] Message delivery failed: {}'.format(err))
    else:
        print('[consumer_map.py] Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def color_producer(val):
    if val >= 50:
        # Minimum value: yellow
        red = hex(round(510 - 5.1*val))[-2:].replace('x', '0')
        green = 'ff'
    else:
        # Max value: yellow
        red = 'ff'
        green = hex(round(5.1*val))[-2:].replace('x', '0')
    return('#' + red + green + '00')


# CODE 
c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 1,
    'auto.offset.reset': 'earliest'
})

time_count = list()
# Iterable
iter_np = range(USERS_TOTAL)
df = np.array([]).reshape(0,21)

col_names = list()
list(map(lambda x: col_names.append('friend_' + str(x)), range(1,MAX_FRIENDS+1)))
df_friends = pd.DataFrame(columns = col_names)
k = 0

## LOOP
while k < 10:
    c.subscribe(['user_data'])
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("[consumer_map.py] Consumer error: {}".format(msg.error()))
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

    # Get user friends
    c.subscribe(['user_score'])
    msg = c.poll(1.0)
    if msg is None:
        print('[consumer_map.py] Waiting...')
        continue
    if msg.error():
        print("[consumer_map.py] Consumer error: {}".format(msg.error()))
        continue

    score = decode_data(msg.value())
    score = ast.literal_eval(score)
    rt_np_score = score[3]

    my_map = folium.Map(
        location= (39.45, -0.35),
        zoom_start = 11.75,
        control_scale = True
    )

    # Add a bubble map to the base map
    for i in range(0,len(rt_np_score)):
        folium.Circle(location = [temp[i][-2], temp[i][-1]], 
                      radius = 5, 
                      fill = True,
                      color = color_producer(rt_np_score[i])).add_to(my_map)

    my_map.save("mapa.html")
    # Print execution time
    time_count.append(time.time() - start_time)
    k += 1
    print('[consumer_map.py] Map generated!')
    # print("--- %s seconds ---" % time_count[-1])

c.close()
print('[consumer_map.py] Demo finished!')