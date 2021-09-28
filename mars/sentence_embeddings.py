"""Functions for embedding sentences with different methods."""

from typing import Dict, List, Union

import numpy as np
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text as text  # Needed for loading universal-sentence-encoder-cmlm/multilingual-preprocess
from laserembeddings import Laser

from mars.db import db_fields
from mars.db.db_fields import EmbeddingType

LABSE_SIZE = 768
LASER_SIZE = 1024

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
