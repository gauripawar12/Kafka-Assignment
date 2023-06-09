import json
import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
# from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List


FILE_PATH = "/Users/thestupidmonk/Downloads/restaurant_orders.csv"
columns = ['order_number', 'order_date', 'item_name', 'quantity', 'product_price', 'total_products']

API_KEY = 'KD73MFTA43GJGYAU'
ENDPOINT_SCHEMA_URL  = 'https://psrc-zy38d.ap-southeast-1.aws.confluent.cloud'
API_SECRET_KEY = 'mg1tiAkfVaAodK7smAbnDbZCsirKQeL/eHSW0XashWO+ScwTjICCwzx82MPLkNIZ'
BOOTSTRAP_SERVER = 'pkc-1dkx6.ap-southeast-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'UQLMVUN3TEKX74QF'
SCHEMA_REGISTRY_API_SECRET = 'lTjW6qT5HeAoOE4Hv9pk8syDO7uHFmPRUJqQREjBC/rE5Qzlk1eUAFIswzG/oav6'


def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                 #  'security.protocol': 'SASL_PLAINTEXT'}
                 'bootstrap.servers': BOOTSTRAP_SERVER,
                 'security.protocol': SECURITY_PROTOCOL,
                 'sasl.username': API_KEY,
                 'sasl.password': API_SECRET_KEY
                 }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,

            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

            }


class Restaurant:
    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

        self.record = record

    @staticmethod
    def dict_to_resta(data: dict, ctx):
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"


def get_restaurant_instance(file_path):
    df = pd.read_csv(file_path)
    df = df.iloc[:,:]
    restaurants: List[Restaurant] = []
    for data in df.values:
        resta = Restaurant(dict(zip(columns, data)))
        restaurants.append(resta)
        yield resta


def resta_to_dict(resta: Restaurant, ctx):

    return resta.record


def delivery_report(err, msg):

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}\n'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    string_serializer = StringSerializer('utf_8')
    my_schema = schema_registry_client.get_latest_version(topic + '-value').schema.schema_str
    json_serializer = JSONSerializer(my_schema, schema_registry_client, resta_to_dict)
    producer = Producer(sasl_conf())

    print("\nLatest Schema \n---------------------\n\n", my_schema, "\n\n---------------------\n")
    
    print("\nProducing user records to topic {}. ^C to exit.\n".format(topic))

    producer.poll(0.0)
    try:
        for res in get_restaurant_instance(file_path=FILE_PATH):
            print(res)
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4()), resta_to_dict),
                             value=json_serializer(res, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("\nInvalid input, discarding record...\n")
        pass

    print("\nFlushing records...\n")
    producer.flush()


main("Restaurant_Topic")

