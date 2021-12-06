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


def load_sentences(key: int) -> List:
    big_number = 1000000

    query = f"FOR u IN {collections.SENTENCES} " \
            f"FILTER TO_NUMBER(SPLIT(u.{SENTENCE_DOC_ID}, \"/\")[1]) == {key} " \
            f"RETURN u"

    sentences = mars.db.database.AQLQuery(query, big_number)
    sentences_in_segment = []
    for j, sentence in enumerate(sentences):
        sentences_in_segment.append(
            {SEQUENCE_NUMBER: sentence[SEQUENCE_NUMBER], SENTENCE_NUMBER: sentence[SENTENCE_NUMBER],
             SENTENCE: sentence[SENTENCE], IS_DEFINITION: sentence[IS_DEFINITION]})
    return sentences_in_segment


@blueprint.route('/<int:key>/sentences')
def get_sentences(key: int):
    result = load_sentences(key)
    result.sort(key=lambda x: (x[SEQUENCE_NUMBER], x[SENTENCE_NUMBER]))

    d = dict().fromkeys([r[SEQUENCE_NUMBER] for r in result])
    for s in result:
        if d[s[SEQUENCE_NUMBER]] is None:
            d[s[SEQUENCE_NUMBER]] = list()
            d[s[SEQUENCE_NUMBER]].append(s[SENTENCE])
        else:
            d[s[SEQUENCE_NUMBER]].append(s[SENTENCE])

    d = list(map(list, d.values()))
    return Response(json.dumps(d), mimetype='application/json')


@blueprint.route('/<int:key>/definitions')
def get_definitions(key: int):
    n = int(request.args.get('n') or 10)
    threshold = float(request.args.get('threshold') or 0.5)
    result = load_sentences(key)

    dict_list = []
    for sentence in result:
        if type(sentence[IS_DEFINITION]) is float and sentence[IS_DEFINITION] > threshold:  # ensure that it is a float
            dict_list.append({'segment': sentence[SEQUENCE_NUMBER],
                              'sentence': sentence[SENTENCE_NUMBER],
                              'probability': sentence[IS_DEFINITION]})
    if len(dict_list) > 0:
        dict_list.sort(key=lambda x: x['probability'], reverse=True)
        return Response(json.dumps(dict_list[:n]), mimetype='application/json')
    else:
        return Response(json.dumps([]))


@blueprint.route('/<int:key>/issues/models')
def get_issues_models(key: int):
    # get first sentence for given document
    # if exists return keys of fields "issues"
    # Example: ["labse", "laser", "keywords"]
    # if not exist then return []
    return Response(json.dumps([]), mimetype='application/json')


@blueprint.route('/<int:key>/issues/<string:model>')
def get_issues(key: int, model: str):
    # Upper limit of sentences for each issue
    n = int(request.args.get('n') or 10)
    # Minimum probability for each sentence
    threshold = float(request.args.get('threshold') or 0.5)

    sentences = load_sentences(key)
    issues = {}

    for sentence in sentences:
        # TODO
        # ex.
        # issues['Fairness'] = []
        # issues['Fairness'].append({'segment': ..., 'sentence': ..., 'probability': ...})
        pass

    for issue, values in issues.items():
        if len(values) > 0:
            values.sort(key=lambda x: x['probability'], reverse=True)
            issues[issue] = values[:n]

    return Response(json.dumps(issues), mimetype='application/json')
