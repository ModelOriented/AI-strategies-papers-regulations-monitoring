import numpy as np
import tqdm
import typer

from mars.db import collections
from mars.db.db_fields import SENTENCE, IS_DEFINITION
from mars.definition_model import DistilBertBaseUncased


def main():
    model = DistilBertBaseUncased("../models/distilbert-base-uncased")

    BATCH_SIZE = 100
    big_number = 1000000000000
    query = collections.sentences.fetchAll(limit=big_number, batchSize=big_number)

    results = np.array(query.result)
    results = np.array_split(results, query.count // BATCH_SIZE)

    for batch_results in tqdm.tqdm(results, total=len(results)):
        sections_to_update = []
        for section in batch_results:
            is_definition = model.predict_single_sentence(section[SENTENCE])
            section[IS_DEFINITION] = float(is_definition)
            sections_to_update.append(section)
        collections.sentences.bulkSave(sections_to_update, onDuplicate="update")


if __name__ == "__main__":
    typer.run(main)
