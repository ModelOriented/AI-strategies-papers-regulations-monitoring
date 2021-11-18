import json
from typing import List

from flask import Blueprint, Response, request

import mars
from mars.db import collections
from mars.db.db_fields import (
    SENTENCE,
    SENTENCE_DOC_ID,
    SENTENCE_NUMBER,
    SEQUENCE_NUMBER,
    IS_DEFINITION
)

blueprint = Blueprint('documents', __name__)


def get_sentences(key: int) -> List:
    big_number = 1000000

    get_sentences = f"FOR u IN {collections.SENTENCES} " \
                    f"FILTER TO_NUMBER(SPLIT(u.{SENTENCE_DOC_ID}, \"/\")[1]) == {key} " \
                    f"RETURN u"

    sentences = mars.db.database.AQLQuery(get_sentences, big_number)
    sentences_in_segment = []
    for j, sentence in enumerate(sentences):
        sentences_in_segment.append(
            {SEQUENCE_NUMBER: sentence[SEQUENCE_NUMBER], SENTENCE_NUMBER: sentence[SENTENCE_NUMBER],
             SENTENCE: sentence[SENTENCE], IS_DEFINITION: sentence[IS_DEFINITION]})
    return sentences_in_segment


@blueprint.route('/<int:key>/sentences')
def get_status(key: int):
    result = get_sentences(key)
    result.sort(key=lambda x: (x[SEQUENCE_NUMBER], x[SENTENCE_NUMBER]))

    d = dict().fromkeys([r[SEQUENCE_NUMBER] for r in result])
    for s in result:
        if d[s[SEQUENCE_NUMBER]] is None:
            d[s[SEQUENCE_NUMBER]] = list()
            d[s[SEQUENCE_NUMBER]].append(s[SENTENCE])
        else:
            d[s[SEQUENCE_NUMBER]].append(s[SENTENCE])

    d = list(map(list, d.values()))
    return Response(json.dumps(d))


@blueprint.route('/<int:key>/definitions')
def get_definitions(key: int):
    n = int(request.args.get('n'))
    result = get_sentences(key)

    dict_list = []
    for sentence in result:
        if type(sentence[IS_DEFINITION]) is float:  # ensure that it is a float
            dict_list.append({'segment': sentence[SEQUENCE_NUMBER],
                              'sentence': sentence[SENTENCE_NUMBER],
                              'probability': sentence[IS_DEFINITION]})
    if len(dict_list) > 0:
        dict_list.sort(key=lambda x: x['probability'], reverse=True)
        return Response(json.dumps(dict_list[:n]))
    else:
        return Response(json.dumps([]))
