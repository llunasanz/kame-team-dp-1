#!/bin/bash
sudo service docker start
docker-compose up -d
wait
python3 sql/create_db.py
wait
python3 Kafka/producer.py &
python3 Kafka/consumer.py &
python3 Kafka/consumer_map.py &