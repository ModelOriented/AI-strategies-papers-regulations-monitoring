import json
import os
from typing import List, Union

import spacy

from mars.config import data_dir
from mars.db import db_fields

try:
    nlp = spacy.load('en_core_web_md')
except OSError:
    os.system("poetry run python -m spacy download en_core_web_md")
    nlp = spacy.load('en_core_web_md')

with open(data_dir + '/keywords/topics.json', 'r') as fp:
    topics = json.load(fp)


class KeywordTopicModel:

    def __init__(self):
        self.topics = topics
        self.name = db_fields.IssueSearchMethod.KEYWORDS

    def score_sentence(self, sentence):

        score = dict.fromkeys(list(topics.keys()))
        doc = nlp(sentence.lower())
        lemmatized_words_list = [token.lemma_ for token in doc]

        for key in score.keys():
            for word in lemmatized_words_list:
                if word in topics[key]:
                    score[key] = 1
            if score[key] is None:
                score[key] = 0

        return score

    def calc_and_save_predictions_for_sentence(
        self, sent
    ):
        if sent[db_fields.ISSUES] is None:
            sent[db_fields.ISSUES] = dict()

        sent[db_fields.ISSUES][self.name] = self.score_sentence(sent.sentence)
        sent.forceSave()