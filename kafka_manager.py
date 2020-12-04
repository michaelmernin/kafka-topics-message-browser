from confluent_kafka import Consumer

from config_handler import ConnectionConfig
from error_handler import ErrorHandler


class ConsumerConnectionManager:
    """ Initializes KafkaConsumer with connection details from main config file"""

    @classmethod
    def initialize_kafka_consumer(cls, environment):
        try:
            settings = cls.__build_consumer_dict(environment)
            return Consumer(settings)
        except Exception as e:
            raise ErrorHandler("Error initializing Kafka Consumer: " + str(e))

    @classmethod
    def __build_consumer_dict(cls, environment):
        try:
            config = ConnectionConfig.connection_details
            return {
                'bootstrap.servers': config['bootstrap.servers'][environment],
                'group.id': config.get('group.id'),
                'client.id': config.get('client.id'),
                'enable.auto.commit': True if config.get('enable.auto.commit').lower() == 'true' else False,
                'session.timeout.ms': int(config.get('session.timeout.ms')),
                'default.topic.config': {
                    'auto.offset.reset': config['default.topic.config']['auto.offset.reset']},
                'security.protocol': config.get('security.protocol'),
                'ssl.key.location': config['ssl'][environment]['ssl.key.location'],
                'ssl.key.password': config['ssl'][environment]['ssl.key.password'],
                'ssl.certificate.location': config['ssl'][environment]['ssl.certificate.location'],
                'ssl.ca.location': config['ssl.ca.location'],
                'enable.partition.eof': False if config.get('enable.partition.eof').lower() == 'false' else True,
                'api.version.request': False if config.get('api.version.request').lower() == 'false' else True
            }
        except KeyError as e:
            raise ErrorHandler("Missing required key from main config file. Missing key: " + str(e))
