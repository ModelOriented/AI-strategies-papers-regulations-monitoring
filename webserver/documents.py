import json
import os

from flask import Blueprint, request, Response, abort
import mars
from mars import config
from mars.db import collections
from mars.db.db_fields import (
    SENTENCE,
    SEGMENT_ID,
    SENTENCE_DOC_ID,
    SEGMENT_DOC_ID,
    IS_DEFINITION,
    ID,
    KEY,
    SEQUENCE_NUMBER,
    SENTENCE_NUMBER
)

from mars.definition_model import DistilBertBaseUncased

blueprint = Blueprint('documents', __name__)

def get_sentence_dict(key):
    big_number = 1000000

    get_segments = f"FOR u IN {collections.SEGMENTED_TEXTS} " \
                   f"FILTER TO_NUMBER(SPLIT(u.{SEGMENT_DOC_ID}, \"/\")[1]) >= {key} " \
                   f"&& TO_NUMBER(SPLIT(u.{SEGMENT_DOC_ID}, \"/\")[1]) <= {key}" \
                   f"RETURN u"

    get_sentences = f"FOR u IN {collections.SENTENCES} " \
                    f"FILTER TO_NUMBER(SPLIT(u.{SENTENCE_DOC_ID}, \"/\")[1]) >= {key} " \
                    f"&& TO_NUMBER(SPLIT(u.{SENTENCE_DOC_ID}, \"/\")[1]) <= {key}" \
                    f"RETURN u"

    sentences = mars.db.database.AQLQuery(get_sentences, big_number)
    segments = mars.db.database.AQLQuery(get_segments, big_number)

    result = {}
    for i, segment in enumerate(segments):
        segment_ID = segment[ID]
        sequence_number = segment[SEQUENCE_NUMBER]

        sentences_in_segment = []
        for j, sentence in enumerate(sentences):
            if sentence[SEGMENT_ID] == segment_ID:
                sentences_in_segment.append({SENTENCE_NUMBER: sentence[SENTENCE_NUMBER], SENTENCE: sentence[SENTENCE]})
        result[sequence_number] = sentences_in_segment
    return result

@blueprint.route('/<string:key>/sentences')
def get_status(key):

    result = get_sentence_dict(key)
    res = []
    for seq in list(result.values()):
        res.append([s['sentence'] for s in seq])

    return Response(json.dumps(res))

@blueprint.route('/<string:key>/definitions')
def get_definitions(key, path_to_model: str = "distilbert-base-uncased"):
    #n = request.args.get('n') # Number of top definitions to return # TODO - nie działa (przynajmniej mi)
    n = 5
    result = get_sentence_dict(key)
    path_to_model = config.models_dir + '/' + path_to_model
    dbc = DistilBertBaseUncased(path_to_model)


    dict_list = []
    for key, seq in result.items():
        for sentence in seq:
            dict_list.append({'segment': key,
                              'sentence': sentence[SENTENCE_NUMBER],
                              'probability': dbc.predict_single_sentence(sentence[SENTENCE])})

    dict_list.sort(key=lambda x: x['probability'], reverse=True)

    return Response(json.dumps(dict_list[:n]))
