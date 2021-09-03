import random
from enum import Enum

from mars.db import collections, db_fields


class ScoringStrategy(str, Enum):
    random = "random"


def score_all(strategy: ScoringStrategy) -> None:
    if strategy == ScoringStrategy.random:
        for doc in collections.annotations.fetchAll():
            doc[db_fields.SENTENCE_SAMPLING_SCORE] = random.random()
    else:
        raise ValueError("Unknown scoring strategy: {}".format(strategy))
    doc.patch()
