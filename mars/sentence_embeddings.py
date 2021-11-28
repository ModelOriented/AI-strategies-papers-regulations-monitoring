"""Functions for embedding sentences with different methods."""

import os
from typing import Dict, List, Union

import numpy as np
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text as text  # Needed for loading universal-sentence-encoder-cmlm/multilingual-preprocess
from laserembeddings import Laser

import mars.db
from mars.db import collections, db_fields, fetch_batches_until_empty
from mars.db.db_fields import EmbeddingType
from mars import logging
from mars.config import models_dir

logger = logging.new_logger(__name__)

LABSE_SIZE = 768
LASER_SIZE = 1024
try:
    laser = Laser()
except FileNotFoundError:
    os.system("poetry run python -m laserembeddings download-models")
    laser = Laser()

labse_preprocessor = hub.KerasLayer(
    models_dir + "/" + "universal-sentence-encoder-cmlm_multilingual-preprocess_2"
)
labse_encoder = hub.KerasLayer(
    models_dir + "/" + "labse2"
)


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


def score_embeddings_for_documents(
    key_min: int, key_max: int, emb_type: db_fields.EmbeddingType = None
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
        for sents_docs in fetch_batches_until_empty(f"FOR u IN {collections.SENTENCES} FILTER u.{db_fields.SENTENCE_DOC_ID} == \"{doc_key}\" && (u.{db_fields.EMBEDDING}.{emb_type} == NULL || u.{db_fields.EMBEDDING}.{emb_type} == NULL) LIMIT 100 RETURN u"):
            logger.info(
                f"Processing doc {doc_key}. Sentences to process in this batch: {len(sents_docs)}"
            )
            sents = [sent_doc[db_fields.SENTENCE] for sent_doc in sents_docs]
            embeddings = embedd_sentences(sents, emb_type)
            for embedding, sent_doc in zip(embeddings, sents_docs):
                if sent_doc[db_fields.EMBEDDING] is None:
                    sent_doc[db_fields.EMBEDDING] = {}
                sent_doc[db_fields.EMBEDDING][emb_type] = list(embedding if type(embedding) == np.ndarray else embedding.numpy())
                sent_doc.forceSave()
