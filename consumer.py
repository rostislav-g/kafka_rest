from kafka import KafkaConsumer
from db import DatabaseConnector
import requests
import json


class Consumer:
    def __init__(self, config):
        self._airflow = config['airflow']
        self._properties = self._get_properties(config)
        self._db = DatabaseConnector(config)

    def run(self) -> None:
        self._consume_from_kafka()

    def _consume_from_kafka(self) -> None:
        # todo: handle ssl parameters
        consumer = KafkaConsumer(**self._properties['consumer'])
        # todo: logging consumer initialization

        for record in consumer:
            print(f'Got next message: {record}')
            # todo: logging "locally record.value input"
            self._db.log_to_db('INFO', record.value, '')
            self.http_post(record.value)

    def http_post(self, data: str) -> None:
        # todo: logging "locally" -> f'Triggering DAG with the following data: {data}'
        dag_name = self._db.get_config_from_db(self.get_message_type(data=data))
        airflow_url = f'{self._airflow}/api/experimental/dags/{dag_name}/dag_runs'

        response = requests.post(
            url=airflow_url,
            data={},
            headers={
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache'
            })

        if response.ok:
            # todo: logging "locally" -> "Triggered Airflow DAG successfully"
            self._db.log_to_db('INFO', data, '')
        else:
            # todo: logging "locally" -> "Error while triggering Airflow DAG"
            self._db.log_to_db('ERROR', data, 'Error while triggering Airflow DAG')

    # todo: check
    @staticmethod
    def get_message_type(data: str) -> str:
        message_type = json.loads()['message_type']
        # todo: add "locally" logging -> f"Got message_type from kafka: {message_type}"
        return message_type

    @staticmethod
    def _get_properties(config):
        properties = {
            'consumer': {
                'topics': config['kafka']['topic'],
                'bootstrap_servers': config['kafka']['host'],
                'group_id': config['kafka']['groupid'],
                'key_deserializer': None,
                'value_deserializer': None,
                'enable_auto_commit': True,
                'auto_offset_reset': 'latest'
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



