from kafka import KafkaProducer
from db import DatabaseConnector
import requests
from json import loads


class Producer:
    def __init__(self, config):
        self._properties = self._get_properties(config)
        self._airflow = config['airflow']
        self._db = DatabaseConnector(config)

    def produce_to_kafka(self, data):
        topic = self._properties['producer']['topic']
        producer = KafkaProducer(**self._properties['producer'])
        # todo: add "locally" logging -> "Sending data to kafka.."
        f = producer.send(topic=topic, value=data) \
            .add_callback(self._on_success, **{'data': data}) \
            .add_errback(self._on_failure, **{'data': data})

    def run(self):
        pass

    def _on_success(self, metadata, data=None) -> None:
        # todo: add "locally" logging -> f"Successfully sent message: {metadata}"
        self._db.log_to_db('INFO', data, '')

    def _on_failure(self, exception, data=None) -> None:
        # todo: add "locally" logging -> f"Error while attempting to send message to kafka: {ex}"
        self._db.log_to_db('ERROR', data, str(exception))


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


