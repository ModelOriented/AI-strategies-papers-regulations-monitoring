from typing import Dict, Tuple

import numpy as np
import pandas as pd
from mars import logging, similarity_calculation
from mars.db import db_fields
from mars.models_training import datasets
from mars.models_training.datasets import DocumentLevelDataset


def transform_to_X_y(
    similarities: Dict[str, Dict[str, float]], labels: pd.DataFrame
) -> Tuple[np.ndarray, np.ndarray]:
    """Transforms similarities dict and labels dataframe to ndarrays for training."""
    df = pd.DataFrame(dict(similarities))
    df_labels = labels[df.columns]
    document_names = df_labels.columns
    X = df[document_names].values.flatten()
    y = df_labels[document_names].values.flatten()
    return X, y


# TODO: @Wisnia tutaj dodaj ten nowy dataset
def load_document_level_issues_dataset(
    dataset: DocumentLevelDataset, emb_type: db_fields.EmbeddingType
) -> Tuple[np.ndarray, np.ndarray]:
    """Load cartesian of documents and targets (in form of similarities) and labels from given dataset.
    X - numpy array of lists of similarities of every sentence in document to given issue;
    y - numpy array of 1/0 label indicating if issue is present in document."""
    logging.debug(f"Loading {dataset.value} dataset...")
    df_labels = datasets.labels[dataset]

    logging.debug("Loading targets similarities...")
    targets = datasets.targets[dataset]
    all_similarities = similarity_calculation.calculate_similarities_to_targets(
        targets, emb_type
    )
    X, y = transform_to_X_y(all_similarities, df_labels)
    return X, y
