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
import mysql.connector

USERS_TOTAL = 100
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
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

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
while True:
    # Get user friends
    c.subscribe(['user_score'])
    msg = c.poll(1.0)
    if msg is None:
        print('Waiting...')
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('here')
    score = decode_data(msg.value())
    score = ast.literal_eval(score)
    rt_np_score = score[3]
    print(rt_np_score)

c.close()
