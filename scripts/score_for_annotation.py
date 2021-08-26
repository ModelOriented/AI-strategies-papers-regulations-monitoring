import random
from enum import Enum

import typer
from mars.db import collections
from mars.db.db_fields import SENTENCE_SAMPLING_SCORE
from mars.db.new_api import database
from tqdm import tqdm


class Strategy(str, Enum):
    random = "random"
    highest_similarity = "highest_similarity"


def score_for_annotation(strategy: Strategy):
    annotations = database[collections.ANNOTATIONS]
    with database.begin_batch_execution() as batch_db:
        annotations_batch = batch_db["Annotations"]
        for d in tqdm(annotations):
            d[SENTENCE_SAMPLING_SCORE] = random.random()
            annotations_batch.update(d)
        print("Applying to database...")
    print("Done!")
