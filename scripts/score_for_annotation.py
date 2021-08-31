import random
from enum import Enum

import typer
from tqdm import tqdm

from mars.db import collections
from mars.db.db_fields import EMBEDDINGS, LASER, QUERY_TARGET, SENTENCE_SAMPLING_SCORE
from mars.db.new_api import database


class Strategy(str, Enum):
    random = "random"
    highest_similarity_laser = "highest_similarity_laser"


def score_for_annotation(strategy: Strategy):
    if strategy == Strategy.highest_similarity_laser:
        from mars.embeddings import embedd_sents_laser, similarity

    target_embeddings = dict()
    annotations = database[collections.ANNOTATIONS]
    with database.begin_batch_execution() as batch_db:
        annotations_batch = batch_db["Annotations"]
        for d in tqdm(annotations):
            if strategy == Strategy.random:
                d[SENTENCE_SAMPLING_SCORE] = random.random()
            elif strategy == Strategy.highest_similarity_laser:
                try:
                    target_embedding = target_embeddings[d[QUERY_TARGET]]
                except KeyError:
                    target_embedding = embedd_sents_laser([d[QUERY_TARGET]])[0]
                    target_embeddings[d[QUERY_TARGET]] = target_embedding
                d[SENTENCE_SAMPLING_SCORE] = similarity(
                    d[EMBEDDINGS][LASER], target_embedding
                )

            annotations_batch.update(d)
        print("Applying to database...")
    print("Done!")


if __name__ == "__main__":
    typer.run(score_for_annotation)
