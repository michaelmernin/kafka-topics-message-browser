from confluent_kafka.serialization import *

from avro_deserializer import Deserializer
from avro_schema_registry_client import RegistryClient
from error_handler import ErrorHandler


class AvroClient:

    def __init__(self, environment):
        try:
            self.registry_client = RegistryClient(environment).registry_client
            self.msg_field = MessageField()
            self.deserializer = None
            self.serial_context = None
        except Exception as e:
            raise ErrorHandler("Unable to initialize AvroClient(): " + str(e))

    def load_deserializer(self, topic_name):
        try:
            self.deserializer = Deserializer(self.registry_client).create_avro_deserializer(topic_name)
            self.serial_context = SerializationContext(topic_name, self.msg_field)
        except Exception as e:
            raise ErrorHandler(
                "Unable to load deserializer for topic: " + topic_name + ". Please check avro schema provided in avro schemas directory:  " + str(
                    e))

    def convert_avro_msg(self, msg):
        try:
            return self.deserializer.__call__(msg.value(), self.serial_context)
        except Exception as e:
            raise ErrorHandler("Error deserializing avro message. Check registry settings: " + str(e))
