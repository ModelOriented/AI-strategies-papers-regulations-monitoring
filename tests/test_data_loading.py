import unittest
from unittest.mock import patch

from mars.db import db_fields
from mars.models_training import data_loading, datasets


class LoadDocumentLevelIssuesDataset(unittest.TestCase):
    @patch("mars.similarity_calculation")
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
