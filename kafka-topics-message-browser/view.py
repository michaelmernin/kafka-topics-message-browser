from flask import jsonify, request, render_template, Blueprint

import constants
from request_handler import RequestHandler

view = Blueprint('view', __name__, url_prefix='')


@view.route('/')
def welcome():
    # (file_ui) application splash page from constants.py
    file_ui = constants.FILE_UI
    return render_template(file_ui)


@view.route('/search', methods=['POST'])
def search():
    response_error_key = constants.RESPONSE_ERROR_KEY
    try:
        # Process request and return final list of json objects
        return jsonify(RequestHandler.process_request(request))
    except Exception as e:
        return jsonify({response_error_key: str(e)})
