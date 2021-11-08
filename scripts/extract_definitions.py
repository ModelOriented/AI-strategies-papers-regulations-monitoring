import numpy as np
import tensorflow as tf
import tqdm
import typer
from transformers import AutoTokenizer
from transformers import TFDistilBertForSequenceClassification

from mars.db import collections
from mars.db.db_fields import SENTENCE, IS_DEFINITION


def predict_single_sentence(sentence: str, model, tokenizer: AutoTokenizer) -> float:
    inputs = tokenizer(sentence, return_tensors="tf")
    inputs["labels"] = tf.reshape(tf.constant(1), (-1, 1))  # Batch size 1
    outputs = model(inputs)
    predictions = tf.math.softmax(outputs.logits, axis=-1)
    return np.array(predictions)[0][1]


def main(TRANSFORMER):
    tokenizer = AutoTokenizer.from_pretrained(TRANSFORMER)

    if TRANSFORMER == "distilbert-base-uncased":
        model = TFDistilBertForSequenceClassification.from_pretrained("../models/" + TRANSFORMER)

    else:
        raise Exception

    BATCH_SIZE = 100
    big_number = 1000000000000
    query = collections.sentences.fetchAll(limit=big_number, batchSize=big_number)

    results = np.array(query.result)
    results = np.array_split(results, query.count // BATCH_SIZE)

    for batch_results in tqdm.tqdm(results, total=len(results)):
        sections_to_update = []
        for section in batch_results:
            is_definition = predict_single_sentence(section[SENTENCE], model, tokenizer)
            section[IS_DEFINITION] = is_definition
            sections_to_update.append(section)
        collections.sentences.bulkSave(sections_to_update, onDuplicate="update")


if __name__ == "__main__":
    TRANSFORMER = "distilbert-base-uncased"
    typer.run(main(TRANSFORMER))
