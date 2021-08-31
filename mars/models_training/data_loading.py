from collections import defaultdict
from enum import Enum
from typing import Tuple

import numpy as np
import pandas as pd
from mars import db, embeddings
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


def load_targets(dataset: DocumenLevelDataset, emb_type: str) -> dict:
    """Load targets for specific dataset, along with their embeddings"""
    tar = targets[dataset]
    target_embeddings = embeddings.get_sentence_to_embedding_mapping(tar, emb_type)
    return target_embeddings


def load_document_level_issues_dataset(
    dataset: DocumenLevelDataset, emb_type: str
) -> Tuple[np.ndarray, np.ndarray]:
    """Load documents, targets and labels from given dataset, along with embeddings"""
    all_similarities = defaultdict(dict)
    target_embeddings = load_targets(dataset, emb_type)
    for text in tqdm(db.collections.processed_texts.fetchAll()):
        for (target_sentence, target_embedding) in target_embeddings.items():
            try:
                scores = list(
                    np.matmul(
                        np.array(text["sentencesEmbeddings"]),
                        np.transpose(target_embedding),
                    )
                )

                all_similarities[text["filename"]][target_sentence] = scores
            except Exception as e:
                print(e)
    all_similarities = dict(all_similarities)
    df = pd.DataFrame(dict(all_similarities))
    df_labels = pd.read_csv("data/labels.csv", index_col=0)
    df_labels = df_labels[df.columns]
    document_names = df_labels.columns
    X = df[document_names].values.flatten()
    y = df_labels[document_names].values.flatten()
    return X, y
