from typing import Tuple

import numpy as np
import pandas as pd
from mars import logging
from mars.db import db_fields
from mars.models_training import datasets
from mars.models_training.datasets import DocumentLevelDataset
from mars.similarity_calculation import calculate_similarities_to_targets


# TODO: @Wisnia tutaj dodaj ten nowy dataset
def load_document_level_issues_dataset(
    dataset: DocumentLevelDataset, emb_type: db_fields.EmbeddingType
) -> Tuple[np.ndarray, np.ndarray]:
    """Load cartesian of documents and targets (in form of similarities) and labels from given dataset.
    X - numpy array of lists of similarities of every sentence in document to given issue
    y - numpy array of 1/0 label indicating if issue is present in document"""
    # TODO: test
    logging.debug(f"Loading {dataset.value} dataset...")
    df_labels = datasets.labels[dataset]

    logging.debug("Loading targets similarities...")
    targets = datasets.targets[dataset]
    all_similarities = calculate_similarities_to_targets(targets, emb_type)
    df = pd.DataFrame(dict(all_similarities))
    df_labels = df_labels[df.columns]
    document_names = df_labels.columns
    X = df[document_names].values.flatten()
    y = df_labels[document_names].values.flatten()
    return X, y
