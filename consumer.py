from kafka import KafkaConsumer
from json import loads

class Consumer:
    def __init__(self, config):
        self.properties = self._get_properties(config)

    def consume_from_kafka(self):
        consumer = KafkaConsumer(**self.properties['consumer'])

        # todo: logging consumer initialization

        for record in consumer:
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

#
# consumer = KafkaConsumer(
#     'sample',
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='latest',
#     enable_auto_commit=True,
#     group_id='test-group',
#     value_deserializer=lambda x: loads(x.decode('utf-8'))
# )
#


