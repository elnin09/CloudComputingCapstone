from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
consumer = KafkaConsumer(
 'testing12345',
 bootstrap_servers=['localhost:9092'],
 auto_offset_reset='earliest',
 enable_auto_commit=True,
 group_id='my-group')
for message in consumer:
    message = message.value
    print('Received.. {}'.format(message))