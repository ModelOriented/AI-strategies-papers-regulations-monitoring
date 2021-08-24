import random

import typer
from mars.db import collections
from mars.db.db_fields import SENTENCE_SAMPLING_SCORE
from mars.db.new_api import database
from tqdm import tqdm

annotations = database[collections.ANNOTATIONS]
with database.begin_batch_execution() as batch_db:
    annotations_batch = batch_db["Annotations"]
    for d in tqdm(annotations):
        d[SENTENCE_SAMPLING_SCORE] = random.random()
        annotations_batch.update(d)
    print("Applying to database...")
print("Done!")
