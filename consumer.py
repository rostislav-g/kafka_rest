from kafka import KafkaConsumer
import requests
from json import loads


class Consumer:
    def __init__(self, config):
        self.properties = self._get_properties(config)
        self.airflow = config['airflow']
        # todo: Add DatabaseConnector implementation
        self.db = None

    def consume_from_kafka(self):
        # todo: handle ssl parameters
        consumer = KafkaConsumer(**self.properties['consumer'])
        # todo: logging consumer initialization

        for record in consumer:
            print(f'Got next message: {record}')
            # todo: logging "locally record.value input"
            # todo: logging DatabaseConnector(configuration).logToDB("INFO", data.value, "")
            self.http_post(record.value)

    def http_post(self, data: str) -> None:
        # todo: logging "locally" -> f'Triggering DAG with the following data: {data}'
        # todoL
        # todo: how to get dag_name? (val airflowURL)
        dag_name = None
        airflow_url = f'{self.airflow}/api/experimental/dags/{dag_name}/dag_runs'

        response = requests.post(
            url=airflow_url,
            data={},
            headers={
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache'
            })

        if response.ok:
            # todo: logging "locally" -> "Triggered Airflow DAG successfully"
            # todo: DatabaseConnector(configuration).logToDB('INFO', data, '')
            pass
        else:
            # todo: logging "locally" -> "Error while triggering Airflow DAG"
            # todo: DatabaseConnector(configurtion).logToDB('ERROR', data, 'Error while Triggering Airflow DAG')
            pass

    # todo: add implementation
    @staticmethod
    def get_message_type(data):
        pass

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



