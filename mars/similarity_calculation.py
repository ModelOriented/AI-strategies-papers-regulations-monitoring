from collections import defaultdict
from typing import Dict, List

import numpy as np
from tqdm import tqdm

from mars import db, logging, sentence_embeddings
from mars.db import db_fields


def similarity(sent_embedding: np.ndarray, query_embedding: np.ndarray) -> float:
    """Calculates similarity between single sentence and query (both provided as embeddings)."""
    return np.matmul(np.array(sent_embedding), np.transpose(query_embedding))


def calculate_similarities_to_targets(
    queries: List[str], emb_type: db_fields.EmbeddingType, key=db_fields.FILENAME
) -> Dict[str, Dict[str, float]]:
    """Calculate similarities of all sentences from all processed texts to given targets.
    Returns mapping: filename (or other choosen key) -> query -> list of similarities of all sentences in text."""
    target_embeddings = sentence_embeddings.get_sentence_to_embedding_mapping(
        queries, emb_type
    )
    all_similarities = defaultdict(dict)
    logging.debug("Loading targets similarities...")
    for processed_text in tqdm(db.collections.processed_texts.fetchAll()):
        if (
            processed_text[db_fields.EMBEDDINGS] is None
            or processed_text[db_fields.EMBEDDINGS][emb_type] is None
        ):
            logging.error(
                f"Missing sentences embedding ({emb_type}) in {processed_text['_id']}"
            )
        else:
            for (target_sentence, target_embedding) in target_embeddings.items():
                try:
                    scores = list(
                        np.matmul(
                            np.array(processed_text[db_fields.EMBEDDINGS][emb_type]),
                            np.transpose(target_embedding),
                        )
                    )
                    all_similarities[processed_text[key]][target_sentence] = scores
                except Exception as e:
                    logging.exception(e)
    all_similarities = dict(all_similarities)

    return all_similarities
