from confluent_kafka import Consumer
from avro.io import DatumReader, BinaryDecoder
import avro.schema
import io

def decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(5)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict

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

    decoded = decode(msg.value())
    print('Received message: {}'.format(decoded))

c.close()