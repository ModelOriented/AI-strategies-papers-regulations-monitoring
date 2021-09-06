import pickle

import numpy as np
from mars.db import collections, db_fields
from mars.db.new_api import database
from mars.models_training import datasets
from mars.similarity_calculation import calculate_similarities_to_targets

model = pickle.load(open("models/ethics_ai_ethics_laser_model.pkl", "rb"))


def predict_for_single_observation(observation):
    return model.predict_proba(np.array([observation]))[0][1]


targets = datasets.targets[datasets.DocumenLevelDataset.ethics_ai_ethics]
all_similarities = calculate_similarities_to_targets(
    targets, db_fields.EmbeddingType.LASER, key=db_fields.TEXT_ID
)

with database.begin_batch_execution() as batch_db:
    for text_id, similarities in all_similarities.items():
        print(f"Processing {text_id}")
        filename = collections.processed_texts.fetchFirstExample(
            {db_fields.TEXT_ID: text_id}
        )[0][db_fields.FILENAME]
        for target, similarities in similarities.items():
            score_for_document = predict_for_single_observation(similarities)
            new_doc = {
                db_fields.TEXT_ID: text_id,
                db_fields.QUERY_TARGET: target,
                db_fields.SIMILARITY_SCORE: score_for_document,
                db_fields.FILENAME: filename,
            }
            batch_db[collections.ANALYTICS].insert(new_doc)
