import random
from enum import Enum

from mars.db import collections, db_fields


class ScoreingStrategy(str, Enum):
    random = "random"


def score_all(strategy: ScoreingStrategy) -> None:
    if strategy == ScoreingStrategy.random:
        for doc in collections.annotations.fetchAll():
            doc[db_fields.SENTENCE_SAMPLING_SCORE] = random.random()
    else:
        raise ValueError("Unknown scoreing strategy: {}".format(strategy))
    doc.patch()
