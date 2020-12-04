import datetime
import json
from datetime import *

import pytz

import constants
import default_constants
from avro_client import AvroClient
from config_handler import ConnectionConfig
from error_handler import ErrorHandler
from kafka_client import KafkaReader
from logger import RequestLogger


class RequestHandler:
    # Default values from default_constants.py
    default_environment = default_constants.DEFAULT_ENVIRONMENT
    default_include_kafka_meta_value = default_constants.DEFAULT_REQUEST_HANDLER_INCLUDE_KAFKA_METADATA
    default_include_delimiter_value = default_constants.DEFAULT_REQUEST_HANDLER_INCLUDE_DELIMITER
    # Final Param Keys
    param_search_string_key = constants.PARAM_SEARCH_STRING_KEY
    param_environment_key = constants.PARAM_ENVIRONMENT_KEY
    param_json_topics_key = constants.PARAM_JSON_TOPICS_KEY
    param_avro_topics_key = constants.PARAM_AVRO_TOPICS_KEY
    param_include_kafka_meta_key = constants.PARAM_INCLUDE_KAFKA_METADATA_KEY
    param_include_delimiter_key = constants.PARAM_INCLUDE_DELIMITER_KEY
    param_other_topic_key = constants.PARAM_OTHER_TOPIC_KEY
    param_not_before_key = constants.PARAM_NOT_BEFORE_KEY
    param_search_count_key = constants.PARAM_SEARCH_COUNT_KEY

    # Incoming Request Keys
    request_search_string_key = constants.REQUEST_SEARCH_STRING_KEY
    request_environment_key = constants.REQUEST_ENVIRONMENT_KEY
    request_json_topics_key = constants.REQUEST_JSON_TOPICS_KEY
    request_avro_topics_key = constants.REQUEST_AVRO_TOPICS_KEY
    request_include_kafka_meta_key = constants.REQUEST_INCLUDE_KAFKA_METADATA_KEY
    request_include_delimiter_key = constants.REQUEST_INCLUDE_DELIMITER_KEY
    request_other_topic_key = constants.REQUEST_OTHER_TOPIC_KEY
    request_not_before_key = constants.REQUEST_NOT_BEFORE_KEY
    request_search_count_key = constants.REQUEST_SEARCH_COUNT_KEY

    # Response keys
    response_error_key = constants.RESPONSE_ERROR_KEY
    response_avro_topics_prefix = constants.RESPONSE_AVRO_TOPICS_PREFIX
    response_json_topics_prefix = constants.RESPONSE_JSON_TOPICS_PREFIX

    @classmethod
    def process_request(cls, request):
        """
        Main method that begins the processing of incoming requests.
        :param request: flask.request()
        :return: list(), json messages that match requested search_string
        """
        # Parse incoming request
        parsed_request = cls.__parse_incoming_request(request)

        # Build final Params dict() used throughout transaction
        params = cls.__build_params(parsed_request)

        # Log final Params dict()
        RequestLogger.log_request(params)

        # Validate Param dict() values, through any invalid request exceptions here
        cls.__validate_params(params)

        # Begin searching transaction
        return cls.__begin_search(params)

    @classmethod
    def __parse_incoming_request(cls, request):
        """
        Parse incoming request convert to dict()
        :return parsed_request dict()
        :rtype dict()
        """
        # Merges form(UI calls), args(Postman form-data) params
        parsed_request = {**request.form.to_dict(), **request.args.to_dict()}

        # Support for JAVA JSON body requests
        if request.json:
            parsed_request = {**parsed_request, **request.json}

        # Support for POSTMAN raw JSON body requests
        if len(parsed_request) == 0:
            parsed_request = json.loads(request.data.decode())

        if len(parsed_request) == 0:
            raise ErrorHandler("Error parsing params")

        return parsed_request

    @classmethod
    def __build_params(cls, parsed_request):
        """
        Build final params dict() with incoming request and default values.
        :return params dict() will be used throughout transaction
        :rtype dict()
        """
        params = {}
        # Search string
        search_string = parsed_request.get(cls.request_search_string_key, None)
        params[cls.param_search_string_key] = search_string.strip().lower() if search_string else None

        # Environment
        params[cls.param_environment_key] = parsed_request.get(cls.request_environment_key,
                                                               cls.default_environment).strip().lower()

        # Include kafka metadata
        params[cls.param_include_kafka_meta_key] = parsed_request.get(cls.request_include_kafka_meta_key,
                                                                      cls.default_include_kafka_meta_value).strip().lower()

        # Visual delimiter
        params[cls.param_include_delimiter_key] = parsed_request.get(cls.request_include_delimiter_key,
                                                                     cls.default_include_delimiter_value).strip().lower()

        # Other topic key
        params[cls.param_other_topic_key] = parsed_request.get(cls.request_other_topic_key, 'none').strip().lower()

        # Search count (not currently being implemented)
        params[cls.param_search_count_key] = int(
            parsed_request.get(cls.request_search_count_key)) if parsed_request.get(
            cls.request_search_count_key) else None

        # Process not before value if passed
        not_before = parsed_request.get(cls.request_not_before_key, 'false').strip().lower()
        if not_before != 'false' and not_before != '':
            not_before = cls.__convert_not_before(not_before)
        params[cls.param_not_before_key] = not_before

        # Build JSON TOPIC SEARCH LIST
        params[cls.param_json_topics_key] = set()
        if parsed_request.get(cls.request_json_topics_key):
            params[cls.param_json_topics_key] = set(parsed_request.get(cls.request_json_topics_key))

        # Search for json topics in keys and values of request (would like to deprecate)
        for name in set(ConnectionConfig.json_topics):
            if name in parsed_request.values() or name in parsed_request.keys():
                params[cls.param_json_topics_key].add(name)

        # Build AVRO TOPIC SEARCH LIST
        params[cls.param_avro_topics_key] = set()
        if parsed_request.get(cls.request_avro_topics_key):
            params[cls.param_avro_topics_key] = set(parsed_request.get(cls.request_avro_topics_key))

        # Search for avro topics in keys and values of request (would like to deprecate)
        for name in set(ConnectionConfig.avro_topics.keys()):
            if name in parsed_request.values() or name in parsed_request.keys():
                params[cls.param_avro_topics_key].add(name)

        # Add other_topic to proper search list (JSON or AVRO)
        if params[cls.param_other_topic_key] != "none":
            if params[cls.param_other_topic_key] in ConnectionConfig.avro_topics:
                params[cls.param_avro_topics_key].add(params[cls.param_other_topic_key])
            else:
                params[cls.param_json_topics_key].add(params[cls.param_other_topic_key])

        return params

    @classmethod
    def __begin_search(cls, params):
        """
        Connect to kafka-broker, iterate and browse requested topics.
        :param params: dict() parsed request
        :return: list(), json messages that match requested search_string
        """
        response = {}
        # Create Kafka consumer
        kafka_reader = KafkaReader(params.get(cls.param_environment_key))

        # Iterate through the json topics and browse all messages,
        # returning all messages that match the search string requested
        for topic_name in params.get(cls.param_json_topics_key):
            try:
                found_msgs = kafka_reader.search_for_msgs(params, topic_name, "json")
            except Exception as e:
                response[cls.response_json_topics_prefix + topic_name] = "Error searching topic. " + str(e)
                continue
            response[cls.response_json_topics_prefix + topic_name] = found_msgs

        # Search avro topics included in request
        if len(params.get(cls.param_avro_topics_key)) > 0:
            # Instantiate avro client in provided environment
            avro_client = AvroClient(params.get(cls.param_environment_key))
            # Set kafka reader's deserializer to above avro client
            kafka_reader.set_avro_deserializer(avro_client)

            for avro_topic_name in params.get(cls.param_avro_topics_key):
                if avro_topic_name not in ConnectionConfig.avro_topics:
                    response[cls.response_avro_topics_prefix + avro_topic_name] = \
                        list().insert(0,
                                      "Error. Application does not have avro schema string for requested topic: " + avro_topic_name)
                    continue
                try:
                    # Load avro deserializer for request topic
                    kafka_reader.avro_deserializer.load_deserializer(avro_topic_name)
                    avro_found_msgs = kafka_reader.search_for_msgs(params, avro_topic_name, "avro")
                except Exception as e:
                    response[cls.response_avro_topics_prefix + avro_topic_name] = "Error searching avro topic.  " + str(
                        e)
                    continue
                response[cls.response_avro_topics_prefix + avro_topic_name] = avro_found_msgs

        # Close connection to queue manager
        try:
            kafka_reader.close()
        except Exception as e:
            response[cls.response_error_key] = "Error closing connection to kafka broker: " + str(e)

        return response

    @classmethod
    def __convert_not_before(cls, not_before):
        """Convert notBefore param into 'aware' datetime object"""
        try:
            return pytz.UTC.localize(datetime.fromisoformat(not_before))
        except Exception as e:
            raise ErrorHandler(
                "Error parsing DateTime 'notBefore' -> " + not_before + " .  Value must be of format "
                                                                        "DateTimeFormatter.ofPattern('yyyy-MM-dd HH:mm:ss') " + str(
                    e))

    @classmethod
    def __validate_params(cls, params):
        """
        Catch and throw all invalid request exceptions here.
        Current Validations:
        - no search_string included in request
        - no valid topics included in request
        """
        # Validate search_string was included in request
        if not params.get(cls.param_search_string_key):
            raise ErrorHandler(
                "Invalid request. Required param: " + cls.request_search_string_key + " not found .... If you are receiving this error in postman "
                                                                                      "and have included the required key, "
                                                                                      "please uncheck option in Headers Content-type - "
                                                                                      "application/x-www-form-urlencoded and try again")

        # Validate valid topics was included in request
        if len(params[cls.param_json_topics_key]) == 0 and len(params.get(cls.param_avro_topics_key)) == 0:
            raise ErrorHandler("Invalid request. No valid topics selected for search")
