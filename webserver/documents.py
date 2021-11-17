import json
from flask import Blueprint, request, Response, abort
import mars
from mars.db import collections
from mars.db.db_fields import (
    SENTENCE,
    SEGMENT_ID,
    SENTENCE_DOC_ID,
    SEGMENT_DOC_ID,
    IS_DEFINITION,
    ID,
    KEY,
    SEQUENCE_NUMBER
)

blueprint = Blueprint('documents', __name__)

@blueprint.route('/<string:key>/sentences')
def get_status(key):
    big_number = 1000000

    print("inside")
    get_segments = f"FOR u IN {collections.SEGMENTED_TEXTS} " \
                      f"FILTER TO_NUMBER(SPLIT(u.{SEGMENT_DOC_ID}, \"/\")[1]) >= {key} " \
                      f"&& TO_NUMBER(SPLIT(u.{SEGMENT_DOC_ID}, \"/\")[1]) <= {key}" \
                      f"RETURN u"

    get_sentences = f"FOR u IN {collections.SENTENCES} " \
                    f"FILTER TO_NUMBER(SPLIT(u.{SENTENCE_DOC_ID}, \"/\")[1]) >= {key} " \
                    f"&& TO_NUMBER(SPLIT(u.{SENTENCE_DOC_ID}, \"/\")[1]) <= {key}" \
                    f"RETURN u"

    sentences = mars.db.database.AQLQuery(get_sentences, big_number)
    print("got segments")

    segments = mars.db.database.AQLQuery(get_segments, big_number)
    print("got sentences")

    # list_sentences = list(sentences)
    # list_segments = list(segments)
    #
    # max_segment = 0
    result = {}
    print("getting inside and parsing")
    for i, segment in enumerate(segments):
        segment_ID = segment[ID]
        sequence_number = segment[SEQUENCE_NUMBER]

        sentences_in_segment = []
        for j, sentence in enumerate(sentences):
            if sentence[SEGMENT_ID] == segment_ID:
                sentences_in_segment.append(sentence[SENTENCE])
        result[sequence_number] = sentences_in_segment
    print("parsed")
    result = list(result.values())

    return Response(json.dumps(result))

@blueprint.route('/<string:key>/definitions')
def get_definitions(key):
    n = request.query.get('n') # Number of top definitions to return
    data = {
        'segment': 0,
        'sentence': 2,
        'probability': 0.7}
    return Response(json.dumps(data))
