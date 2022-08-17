from typing import List, Union

import numpy as np

from mair import logging, sentence_embeddings

logger = logging.new_logger(__name__)


def similarity(sent_embedding: np.ndarray, query_embedding: np.ndarray) -> float:
    """Calculates similarity between single sentence and query (both provided as embeddings)."""
    return (
        1
        + np.matmul(np.array(sent_embedding), np.transpose(query_embedding))
        / (np.linalg.norm(sent_embedding) * np.linalg.norm(query_embedding))
    ) / 2