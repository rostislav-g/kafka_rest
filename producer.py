from kafka import KafkaProducer
import requests
from json import loads


class Producer:
    def __init__(self, config):
        self.properties = self._get_properties(config)
        self.airflow = config['airflow']
        # todo: Add DatabaseConnector implementation
        self.db = None

    def produce_to_kafka(self, data):
        # todo: add implementation of callback function
        callback = None
        topic = self.properties['producer']['topic']

        producer = KafkaProducer(**self.properties['producer'])
        producer \
            .send(topic=topic, value=data) \
            .add_callback(callback, **{'callback_parameter': 'value'})

    def run(self):
        pass

    @staticmethod
    def _get_properties(config):
        properties = {
            'producer': {
                'topics': config['kafka']['topic'],
                'bootstrap_servers': config['kafka']['host'],
                'key_serializer': None,
                'value_serializer': None,
                'acks': 'all'
            },
        }
        if config['kafka']['protocol'].lower() == 'ssl':
            properties['common_client'] = 'SSL',
            properties['ssl'] = {
                'ssl_endpoint_identification_algorithm': '',
                'ssl_truststore_location': config['kafka']['truststoreLocation'],
                'ssl_keystore_location': config['kafka']['keystoreLocation'],
                'ssl_truststore_password': config['kafka']['truststorePassword'],
                'ssl_keystore_password': config['kafka']['keystorePassword'],
                'ssl_key_password': config['kafka']['keyPassword']
            }

        return properties


