"""Functions for embedding sentences with different methods."""

import os
from typing import Dict, Iterator, List, Union

import numpy as np
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text as text  # Needed for loading universal-sentence-encoder-cmlm/multilingual-preprocess
from laserembeddings import Laser

import mars.db
from mars.db import collections, db_fields
from mars.db.db_fields import EmbeddingType
from mars import logging

logger = logging.new_logger(__name__)

LABSE_SIZE = 768
LASER_SIZE = 1024
try:
    laser = Laser()
except FileNotFoundError:
    os.system("poetry run python -m laserembeddings download-models")
    laser = Laser()

labse_preprocessor = hub.KerasLayer(
    "https://tfhub.dev/google/universal-sentence-encoder-cmlm/multilingual-preprocess/2"
)
labse_encoder = hub.KerasLayer("https://tfhub.dev/google/LaBSE/2")


def _normalization(embeds):
    norms = np.linalg.norm(embeds, 2, axis=1, keepdims=True)
    return embeds / norms


def embedd_sentences(
    sents: Union[List[str], str], embedding_type: EmbeddingType
) -> np.ndarray:
    """Calculates embeddings for given sentence list, or single sentence."""
    if embedding_type == EmbeddingType.LABSE:
        if isinstance(sents, str):
            sents = [sents]
        embds = labse_encoder(labse_preprocessor(tf.constant(sents)))["default"]
    elif embedding_type == EmbeddingType.LASER:
        embds = laser.embed_sentences(sents, lang="en")
    else:
        raise ValueError("Unknown embedding type")
    return _normalization(embds)


def get_sentence_to_embedding_mapping(
    sentences: List[str],
    emb_type: db_fields.EmbeddingType = db_fields.EmbeddingType.LABSE,
) -> Dict[str, np.ndarray]:
    # TODO: tests, also for types
    embds = embedd_sentences(sentences, emb_type)
    target_embeddings = dict()
    for emb, targ in zip(embds, sentences):
        target_embeddings[targ] = emb
    return target_embeddings


def fetch_batches_until_empty(
    collection, filter: dict, batch_size=1000
) -> Iterator[list]:
    """Fetch collection in batches. Stop fetching when there is no fields after filtering"""
    finished = False
    batch = 0
    while not finished:
        batch += 1
        logger.info("Fetching next batch: %s", batch)
        results = [
            d for d in collection.fetchByExample(filter, batch_size, limit=batch_size)
        ]
        if len(results) != 0:
            yield results
        else:
            finished = True
            logger.info("Finished")


def score_embeddings_for_documents(
    key_min: int, key_max: int, emb_type: db_fields.EmbeddingType
):
    if emb_type is None:
        score_embeddings_for_documents(key_min, key_max, EmbeddingType.LABSE)
        score_embeddings_for_documents(key_min, key_max, EmbeddingType.LASER)
        return
    all_docs_between_keys = f"""FOR d in {collections.DOCUMENTS}
        FILTER TO_NUMBER(d._key)>={key_min} && TO_NUMBER(d._key)<={key_max}
        return d._id"""
    todo_docs = mars.db.database.AQLQuery(all_docs_between_keys, 10000, rawResults=True)
    logger.info("All docs: %s", len(todo_docs))
    for doc_key in todo_docs:
        for sents_docs in fetch_batches_until_empty(
            collections.sentences,
            {db_fields.SENTENCE_DOC_ID: doc_key, db_fields.EMBEDDING: {emb_type: None}},
            100,
        ):
            logger.info(
                f"Processing doc {doc_key}. Sentences to process in this batch: {len(sents_docs)}"
            )
            sents = [sent_doc[db_fields.SENTENCE] for sent_doc in sents_docs]
            embeddings = embedd_sentences(sents, emb_type)
            for embedding, sent_doc in zip(embeddings, sents_docs):
                sent_doc[db_fields.EMBEDDING][emb_type] = list(embedding.numpy())
                sent_doc.patch()
