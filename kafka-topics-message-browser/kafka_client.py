import json

from confluent_kafka import KafkaError, TopicPartition

import constants
from error_handler import ErrorHandler
from kafka_manager import ConsumerConnectionManager


# Used if using timestamp filtering functionality (see commented section below)
# from dateutil.parser import parse


class KafkaReader:
    """ Handles connection to topic and polling """

    param_search_string_key = constants.PARAM_SEARCH_STRING_KEY
    param_include_delimiter_key = constants.PARAM_INCLUDE_DELIMITER_KEY
    param_include_kafka_meta_key = constants.PARAM_INCLUDE_KAFKA_METADATA_KEY

    def __init__(self, environment):
        self.avro_deserializer = None
        try:
            self.consumer = ConsumerConnectionManager.initialize_kafka_consumer(environment)
            try:
                # Retrieve list of topics from KafkaConsumer(). timeout is set as float(10).
                # Timeout errors typically signify a connection error to kafka-broker.
                self.consumer_topic_list = self.consumer.list_topics(timeout=float(10)).topics
            except Exception as e:
                raise ErrorHandler(
                    "Error retrieving list of available topics. Check Kafka connection settings. Error: " + str(
                        e))
        except Exception as e:
            raise ErrorHandler("Error initializing KafkaReader(): " + str(e))

    def close(self):
        """Close connection to kafka broker"""
        self.consumer.close()

    def set_avro_deserializer(self, avro_client):
        self.avro_deserializer = avro_client

    def __retrieve_partition_data(self, topic):
        """
        Returns patition data for given topic.
        :param topic: string, name of topic being searched
        :return: confluent_kafka.TopicPartition()
        """
        if topic not in self.consumer_topic_list:
            raise ErrorHandler("Application does not have access to requested topic: " + topic)
        try:
            return self.consumer_topic_list.get(topic).partitions
        except Exception as e:
            raise ErrorHandler("Error retrieving partition data: " + str(e))

    def search_for_msgs(self, request_params, topic, message_type):
        """
        Method handles browsing of request topic's partitions and passes messages to given message type's parser.
        Generic parsers (json and avro) and provided below.  You may add additional parsers to fit your needs
        :param request_params: dict()
        :param topic: string, name of topic being search
        :param message_type: string, type of messages being polled and parses
        :return: list() of all messages in given topic that include request search_string
        """
        messages = []
        partitions = self.__retrieve_partition_data(topic)
        for k, partition in partitions.items():
            self.consumer.assign([TopicPartition(topic, partition.id)])
            while True:
                try:
                    msg = self.consumer.poll(.5)
                    if msg is None:
                        continue
                    elif not msg.error():
                        parsed_msg = None
                        # Add more message types here if desired. out of box only provided json and avro types
                        if message_type == 'json':
                            parsed_msg = self.__parse_json_msg(request_params, msg)
                        elif message_type == 'avro':
                            parsed_msg = self.__parse_avro_msg(request_params, msg)
                        if parsed_msg:
                            messages.insert(0, parsed_msg)
                            if request_params.get(self.param_include_delimiter_key) == 'true':
                                messages.insert(1,
                                                "##############################################################")
                                messages.insert(2,
                                                "####################  MESSAGE SEPARATOR  #####################")
                                messages.insert(3,
                                                "##############################################################")

                    elif msg.error():
                        # If Kafka end of partition error received,
                        # unassign consumer from partition and continue iteration.
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            self.consumer.unassign()
                            break
                        else:
                            # currently not processing message errors, other than KafkaError.PARTITION_EOF.
                            # If desired, add this logic here.
                            error_msg_received = msg.error()
                            continue
                except ErrorHandler as e:
                    raise ErrorHandler("Error parsing message. " + str(e))

                # Ignore generic exceptions, as there may be malformed messages in topic.
                # If you would like an exception thrown please add here.
                except Exception as e:
                    continue

        return messages

    def __parse_json_msg(self, request_params, msg):
        """
        Generic json message parser provided below.  You must add your customer logic here
        Parses polled json message.  Add metadata if enabled.
        :param request_params:
        :param msg: confluent_kafka.Message()
        :return: dict() data, if contains requested search_string.  Else returns None
        """
        data = json.loads(msg.value())
        if request_params.get(self.param_search_string_key) in str(data).lower():
            if request_params.get(self.param_include_kafka_meta_key) == 'true':
                data['additional_added_metadata'] = dict()
                try:
                    key_data = msg.key().decode()
                    if key_data.startswith("{"):
                        data['additional_added_metadata']['key'] = json.loads(msg.key())
                    else:
                        data['additional_added_metadata']['key'] = key_data

                    data['additional_added_metadata']['another_example'] = msg.another_example()
                except Exception as e:
                    msg['additional_added_metadata']['key'] = "Error retrieving key: " + str(e)
            return data
        else:
            return None

    def __parse_avro_msg(self, request_params, msg):
        """
        Generic avro message parser provided below.  You must add your customer logic here
        Parses polled avro message.  Add metadata if enabled.
        :param request_params: dict()
        :param msg: confluent_kafka.Message()
        :return: dict() data, if contains requested search_string.  Else returns None
        """
        data = self.avro_deserializer.convert_avro_msg(msg)
        if request_params.get(self.param_search_string_key) in str(data).lower():
            if request_params.get(self.param_include_kafka_meta_key) == 'true':
                data['additional_added_metadata'] = dict()
                try:
                    data['additional_added_metadata']['key'] = msg.key().decode()
                    data['additional_added_metadata']['another_example'] = msg.another_example()
                except Exception as e:
                    data['additional_added_metadata'][
                        'error'] = "Error extracting metadata: " + str(e)
            return data
        else:
            return None
