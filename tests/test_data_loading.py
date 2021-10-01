import unittest
from unittest.mock import patch

import pandas as pd
from mars.db import db_fields
from mars.models_training import data_loading, datasets

SIMILARITIES = {
    "doc1": {"query1": [1, 2, 3, 4], "query2": [5, 6, 7, 8]},
    "doc2": {"query1": [1, 1, 1, 1], "query2": [3, 3, 3, 3]},
    "doc3": {"query1": [2, 2, 2, 2], "query2": [4, 4, 4, 4]},
}

LABELS = pd.DataFrame.from_dict(
    {"query1": [1, 0, 1], "query2": [0, 1, 1]},
    orient="index",
    columns=["doc1", "doc2", "doc3"],
)


class DataLoading(unittest.TestCase):
    @patch(
        "mars.models_training.data_loading.similarity_calculation.calculate_similarities_to_targets"
    )
    def test_loads_ethics_ai_dataset(self, mock_similarity_calculation):

        X, y = data_loading.load_document_level_issues_dataset(
            datasets.DocumentLevelDataset.ethics_ai_ethics,
            db_fields.EmbeddingType.LABSE,
        )
        mock_similarity_calculation.assert_called_once_with(
            datasets.targets[datasets.DocumentLevelDataset.ethics_ai_ethics],
            db_fields.EmbeddingType.LABSE,
        )


if __name__ == "__main__":
    unittest.main()
