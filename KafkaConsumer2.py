import argparse

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


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
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Restaurant:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_restaurant(data:dict,ctx):
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    my_schema_str = schema_registry_client.get_latest_version(topic+'-value').schema.schema_str

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    json_deserializer = JSONDeserializer(my_schema_str,
                                         from_dict=Restaurant.dict_to_restaurant)

    
    count = 0
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            restaurant = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if restaurant is not None:
                count += 1
                print("User record {}: restaurant: {}\n"
                      .format(msg.key(), restaurant))
        except KeyboardInterrupt:
            break

    consumer.close()

    print("\nNo of values it consumed from the topic is --------> {}".format(count))


main("Restaurant_Topic")
