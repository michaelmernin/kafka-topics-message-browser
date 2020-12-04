import csv
import io
import json
import logging
import os
from copy import deepcopy

import constants
import default_constants
from config_handler import ConnectionConfig


class RequestLogger:
    """Implements logger, create directories and log file, and triggers logs"""
    # Default logger config value from default_contants.py
    default_logger_path = default_constants.DEFAULT_LOGGER_PATH
    default_logger_disablement = default_constants.DEFAULT_LOGGER_ENABLED
    # Logger configs from constants.py
    logger_name = constants.LOGGER_NAME
    logger_disable_key = constants.LOGGER_DISABLE_KEY
    logger_path_key = constants.LOGGER_PATH_KEY
    # Param dict() keys
    json_topics = constants.PARAM_JSON_TOPICS_KEY
    avro_topics = constants.PARAM_AVRO_TOPICS_KEY
    search_string = constants.PARAM_SEARCH_STRING_KEY

    @classmethod
    def log_request(cls, params):
        """
        Log parsed incoming request
        :param params: dict()
        """
        # convert all sets into lists, from params, and log message
        params_copy = deepcopy(params)
        params_copy[cls.json_topics] = list(params_copy[cls.json_topics])
        params_copy[cls.avro_topics] = list(params_copy[cls.avro_topics])
        logger = logging.getLogger(cls.logger_name)
        logger.debug(json.dumps(params_copy))

    @classmethod
    def create_logger(cls):
        """Create logger, if enabled, at application boot time"""
        logger = logging.getLogger(cls.logger_name)
        # Check if logger has been enabled, if false disable logger.
        if ConnectionConfig.logger_details.get(cls.logger_disable_key,
                                               cls.default_logger_disablement).lower() == 'true':
            # Creates log file at location specified in logger_config.yml ('logger.requests.path)
            # or uses default value provided above (default_logger_path)
            try:
                log_filepath = ConnectionConfig.logger_details.get(cls.logger_path_key, cls.default_logger_path)
                os.makedirs(os.path.dirname(log_filepath), exist_ok=True)
            except Exception as e:
                raise Exception("Error creating log file. Check log path provided: " + log_filepath)
            fh = logging.FileHandler(log_filepath)
            logger.addHandler(fh)
            logger.handlers[0].setFormatter(CsvFormatter())
            logger.setLevel(logging.DEBUG)
            logger.propagate = False
        else:
            logger.disable = True


class RequestLoggerDialect(csv.Dialect):
    delimiter = "\n"
    doublequote = False
    escapechar = ' '
    lineterminator = "\n"
    quotechar = ""
    quoting = csv.QUOTE_NONE


class CsvFormatter(logging.Formatter):
    """Log formatter.  You will have access to Param dict()"""
    # Param keys
    search_string = constants.PARAM_SEARCH_STRING_KEY
    environment = constants.PARAM_ENVIRONMENT_KEY
    json_topics = constants.PARAM_JSON_TOPICS_KEY
    avro_topics = constants.PARAM_AVRO_TOPICS_KEY

    def __init__(self):
        super().__init__()
        self.output = io.StringIO()
        self.writer = csv.writer(self.output, dialect=RequestLoggerDialect)

    def format(self, record):
        timestamp = self.formatTime(record).replace(",", "+")
        dict_record = json.loads(record.msg)
        self.writer.writerow(["timestamp: " + timestamp,
                              "search_param: " + dict_record[self.search_string],
                              "environment: " + dict_record[self.environment],
                              "topics_searched: " + str(
                                  dict_record[self.json_topics] + dict_record[self.avro_topics]),
                              "full_request: " + record.msg,
                              "---------------------------------------------",
                              "---------------------------------------------"
                              ])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()
