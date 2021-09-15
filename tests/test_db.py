import unittest
from mars import db
from mars.db import db_fields, collections
import pyArango


class DBMethods(unittest.TestCase):
    def test_non_existing_document(self):
        is_present = db.is_document_present("none")
        self.assertFalse(is_present)

    def test_collections(self):
        self.assertIsInstance(
            collections.processed_texts, pyArango.collection.Collection
        )
        self.assertIsInstance(
            collections.document_sources, pyArango.collection.Collection
        )
        self.assertIsInstance(collections.contents, pyArango.collection.Collection)
        self.assertIsInstance(
            collections.search_targets, pyArango.collection.Collection
        )
        self.assertIsInstance(collections.annotations, pyArango.collection.Collection)

    def test_db_fields(self):
        self.assertIsNotNone(db_fields.EmbeddingType)
        self.assertIsNotNone(db_fields.EmbeddingType)
        self.assertIsNotNone(db_fields.EmbeddingType)
        self.assertIsNotNone(db_fields.EmbeddingType)


if __name__ == "__main__":
    unittest.main()
