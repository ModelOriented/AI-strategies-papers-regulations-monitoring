from typing import Dict, List

import numpy as np
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text as text  # Needed for loading universal-sentence-encoder-cmlm/multilingual-preprocess
from laserembeddings import Laser

from mars.db import db_fields

laser = Laser()

preprocessor = hub.KerasLayer(
    "https://tfhub.dev/google/universal-sentence-encoder-cmlm/multilingual-preprocess/2"
)
encoder = hub.KerasLayer("https://tfhub.dev/google/LaBSE/2")


def normalization(embeds):
    norms = np.linalg.norm(embeds, 2, axis=1, keepdims=True)
    return embeds / norms


def embedd_sents_labse(sents):
    return normalization(encoder(preprocessor(tf.constant(sents)))["default"]).numpy()


def embedd_sents_laser(sents):
    return normalization(laser.embed_sentences(sents, lang="en"))


def similarity(sent_embedding: np.ndarray, query_embedding: np.ndarray) -> float:
    return np.matmul(np.array(sent_embedding), np.transpose(query_embedding))


def embedd_sentences(sents: List[str], embedding_type: db_fields.EmbeddingType):
    if embedding_type == db_fields.EmbeddingType.LABSE:
        embds = embedd_sents_labse(sents)
    elif embedding_type == db_fields.EmbeddingType.LASER:
        embds = embedd_sents_laser(sents)
    else:
        raise ValueError("Unknown embedding type")
    return embds


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
