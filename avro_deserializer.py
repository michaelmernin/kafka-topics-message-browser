from confluent_kafka.schema_registry.avro import AvroDeserializer

import constants
from config_handler import ConnectionConfig
from error_handler import ErrorHandler


class Deserializer:
    directory_avro_schemas = constants.DIRECTORY_AVRO_SCHEMAS

    def __init__(self, registry_client):
        self.config_avro_location = ConnectionConfig.avro_topics
        self.registry_client = registry_client

    def create_avro_deserializer(self, topic_name):
        schema_string = self.load_avro_schema_string(topic_name)
        return AvroDeserializer(schema_string, self.registry_client)

    def load_avro_schema_string(self, topic_name):
        if topic_name not in self.config_avro_location:
            raise ErrorHandler("Error. Application does not have avro schema for requested topic")
        try:
            with open(self.directory_avro_schemas + "/" + self.config_avro_location.get(topic_name),
                      'r') as schema_file:
                return schema_file.read().replace('\n', '')
        except Exception as e:
            raise ErrorHandler("Error. Unable to load schema: " + str(e))
