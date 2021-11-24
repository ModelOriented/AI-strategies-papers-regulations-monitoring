import mars.sentence_embeddings
from mars.db import collections
from mars.sentence_embeddings import EmbeddingType, embedd_sentences

del mars.sentence_embeddings.laser

from typing import Iterator



def fetch_batches_until_empty(
    collection, filter: dict, batch_size=1000
) -> Iterator[list]:
    """Fetch collection in batches. Stop fetching when there is no fields after filtering"""
    finished = False
    batch = 0
    while not finished:
        batch += 1
        print("Fetching next batch:", batch)
        results = [
            d for d in collection.fetchByExample(filter, batch_size, limit=batch_size)
        ]
        if len(results) != 0:
            yield results
        else:
            finished = True
            print("Finished")


def fetch_in_batches(collection, batch_size=100):
    skip = 0
    finished = False
    while not finished:
        results = [d for d in collection.fetchAll(limit=batch_size, skip=skip)]
        print("Processed:", skip)
        if len(results) != 0:
            yield results
        else:
            finished = True


for sents_docs in fetch_batches_until_empty(
    collections.sentences, {EMBEDDING: None}, 100
):
    sents = [sent_doc["sentence"] for sent_doc in sents_docs]
    embeddings = embedd_sentences(sents, EmbeddingType.LABSE)

    for embedding, sent_doc in zip(embeddings, sents_docs):
        sent_doc[EMBEDDING] = list(embedding.numpy())
        sent_doc.patch()
