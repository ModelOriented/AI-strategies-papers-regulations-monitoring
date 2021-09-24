from collections import defaultdict
from typing import Dict, List

import numpy as np
from tqdm import tqdm

from mars import db, embeddings, logging
from mars.db import db_fields


def calculate_similarities_to_targets(
    queries: List[str], emb_type: db_fields.EmbeddingType, key=db_fields.FILENAME
) -> Dict[str, Dict[str, float]]:
    """Calculate similarities of all sentences from all processed texts to given targets.
    Returns mapping filename (or other choosen key) -> query -> list of similarities of all sentences in text."""
    target_embeddings = embeddings.get_sentence_to_embedding_mapping(queries, emb_type)
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
