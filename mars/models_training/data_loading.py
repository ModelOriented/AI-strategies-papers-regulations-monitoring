from collections import defaultdict
from enum import Enum
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from mars import db, embeddings, logging
from tqdm import tqdm


class DocumenLevelDataset(Enum):
    ethics_ai_ethics = "hagendorffEthicsAIEthics2020"


targets = {
    DocumenLevelDataset.ethics_ai_ethics: [
        "privacy protection",
        "fairness",
        "accountability",
        "transparency, openness",
        "safety, cybersecurity",
        "common good, sustainability, well-being",
        "human oversight, control, auditng",
        "solidarity, inclusion, social cohesion",
        "explainability, interpretabiliy",
        "science-policy link",
        "legislative framework, legal status of AI systems",
        "future of employment/worker rights",
        "responsible/intensified research funding",
        "public awareness, education about AI and its risks",
        "dual-use problem, military, AI arms race",
        "field-specific deliberations (health, military, mobility etc.)",
        "human autonomy",
        "diversity in the field of AI",
        "certification for AI products",
        "protection of whistleblowers",
        "cultural differences in the ethically aligned design of AI systems",
        "hidden costs (labeling, clickwork, contend moderation, energy, resources)",
    ]
}


def load_targets(dataset: DocumenLevelDataset, emb_type: str) -> Dict[str, np.ndarray]:
    """Load targets for specific dataset, along with their embeddings."""
    tar = targets[dataset]
    target_embeddings = embeddings.get_sentence_to_embedding_mapping(tar, emb_type)
    return target_embeddings


def load_document_level_issues_dataset(
    dataset: DocumenLevelDataset, emb_type: str
) -> Tuple[np.ndarray, np.ndarray]:
    """Load cartesian of documents and targets (in form of similarities) and labels from given dataset.
    X - numpy array of lists of similarities of every sentence in document to given issue
    y - numpy array of 1/0 label indicating if issue is present in document"""
    # TODO: test
    logging.debug(f"Loading {dataset.value} dataset...")
    df_labels = pd.read_csv("data/labels.csv", index_col=0)

    all_similarities = defaultdict(dict)
    logging.debug("Loading targets similarities...")
    target_embeddings = load_targets(dataset, emb_type)
    for processed_text in tqdm(db.collections.processed_texts.fetchAll()):
        if (
            processed_text["embeddings"] is None
            or processed_text["embeddings"][emb_type] is None
        ):
            logging.error(
                f"Missing sentences embedding ({emb_type}) in {processed_text['_id']}"
            )
        else:
            for (target_sentence, target_embedding) in target_embeddings.items():
                try:
                    scores = list(
                        np.matmul(
                            np.array(processed_text["embeddings"][emb_type]),
                            np.transpose(target_embedding),
                        )
                    )
                    all_similarities[processed_text["filename"]][
                        target_sentence
                    ] = scores
                except Exception as e:
                    logging.exception(e)
    all_similarities = dict(all_similarities)
    df = pd.DataFrame(dict(all_similarities))
    df_labels = df_labels[df.columns]
    document_names = df_labels.columns
    X = df[document_names].values.flatten()
    y = df_labels[document_names].values.flatten()
    return X, y
