import json
from flask import Blueprint, request, Response, abort

blueprint = Blueprint('documents', __name__)

@blueprint.route('/<string:key>/sentences')
def get_status(key):
    return Response(json.dumps(
        [
        ['First sentence', 'Next sentence'], # First segment
        ['Some Sentence'] # Second segment
        ]
    ))

@blueprint.route('/<string:key>/definitions')
def get_definitions(key):
    n = request.query.get('n') # Number of top definitions to return
    data = [
        'segment': 0,
        'sentence': 2,
        'probability': 0.7
    ]
    return Response(json.dumps(data))
