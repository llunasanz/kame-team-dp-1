from confluent_kafka import Consumer
from avro.io import DatumReader, BinaryDecoder
import avro.schema
import io
import base64
import numpy as np
import json
import ast

USERS_TOTAL = 100
iter_np = range(USERS_TOTAL)

# Receive and decode bytes to numpy array
def decode_data(j_dumps:str):
    # Convert Base 64 representation to byte representation
    data = base64.b64decode(j_dumps).decode()
    return data

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 0,
    'auto.offset.reset': 'earliest'
})

c.subscribe(['numtest'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    # decoded = decode(msg.value())
    # print('Received message: {}'.format(receive_and_decode_bytes_to_numpy_array(msg.value())))
    # print(json.loads(msg.value))

    temp = decode_data(msg.value())
    temp = ast.literal_eval(temp)
    temp = np.array(list(map(lambda v: list(list(temp.values())[v].values()), iter_np)), dtype=object)
    temp = np.array(list(map(lambda v: np.delete(np.append(temp[v], [temp[v][4].get('lat'), temp[v][4].get('lon')]), 4), iter_np)))
    print(type(temp))

c.close()