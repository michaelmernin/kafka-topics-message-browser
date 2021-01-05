import yaml
from flask import Flask
from waitress import serve

import constants
import view
from config_handler import ConnectionConfig
from logger import RequestLogger

# Get constants from constants.py
configuration_directory = constants.DIRECTORY_CONFIGURATIONS
topics_directory = constants.DIRECTORY_TOPICS
file_main_config = constants.FILE_MAIN_CONFIG
file_avro_topics = constants.FILE_AVRO_TOPICS
file_logger_config = constants.FILE_LOGGER_CONFIG

# Load main config file
with open(configuration_directory + "\\" + file_main_config) as file:
    ConnectionConfig.connection_details = yaml.load(file, Loader=yaml.FullLoader)
# Load avro topics
with open(topics_directory + "\\" + file_avro_topics) as file:
    ConnectionConfig.avro_topics = yaml.load(file, Loader=yaml.FullLoader) or {}
# Load logger config file
with open(configuration_directory + "\\" + file_logger_config) as file:
    ConnectionConfig.logger_details = yaml.load(file, Loader=yaml.FullLoader) or {}

app = Flask(__name__)

app.register_blueprint(view.view)

if __name__ == '__main__':
    # initialize logger, create directory and log file
    RequestLogger.create_logger()
    # app.debug = True Allows for pretty print in response
    app.debug = True
    app.config["JSON_SORT_KEYS"] = False
    app.config['DEFAULT_PARSERS'] = [
        'flask.ext.api.parsers.JSONParser',
        'flask.ext.api.parsers.URLEncodedParser',
        'flask.ext.api.parsers.MultiPartParser'
    ]
    # Production WSGI server "waitress"
    serve(app, host=ConnectionConfig.connection_details.get('hostname'),
          port=ConnectionConfig.connection_details.get('port'))

    # Flask built in server (can use when running locally)
    # app.run()
