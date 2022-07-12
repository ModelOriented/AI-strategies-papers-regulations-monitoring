"""Functions for embedding sentences with different methods."""

import os
from typing import Dict, List, Union

import numpy as np
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text as text  # Needed for loading universal-sentence-encoder-cmlm/multilingual-preprocess
from laserembeddings import Laser

import mair.db
from mair import logging
from mair.config import models_dir
from mair.db import collections, db_fields, fetch_batches_until_empty
from mair.db.db_fields import EmbeddingType

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
labse_encoder = hub.KerasLayer(models_dir + "/" + "labse2")


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
