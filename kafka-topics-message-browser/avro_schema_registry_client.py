from confluent_kafka.schema_registry import SchemaRegistryClient

from config_handler import ConnectionConfig
from pfx_to_pem_handler import PFXReader


class RegistryClient:

    def __init__(self, environment):
        self.registry_client = self.create_schema_registry_client(environment)

    def create_schema_registry_client(self, environment):
        pfx_reader = PFXReader()
        with pfx_reader.pfx_to_pem(environment) as certX:
            schema_client_settings = {
                "url": ConnectionConfig.connection_details['schema_registry_url'][environment],
                'ssl.key.location': certX,
                'ssl.certificate.location': ConnectionConfig.connection_details['ssl'][environment][
                    'ssl.certificate.location'],
                'ssl.ca.location': ConnectionConfig.connection_details['ssl.ca.location']
            }

        return SchemaRegistryClient(schema_client_settings)
