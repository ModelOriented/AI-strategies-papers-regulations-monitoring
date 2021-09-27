import unittest

from mars import sentence_embeddings


class EmbeddSentences(unittest.TestCase):
    def test_labse_embedding_size(self):
        sentences = ["This is a test sentence", "Another one"]
        embedding = sentence_embeddings.embedd_sentences(
            sentences, sentence_embeddings.EmbeddingType.LABSE
        )
        self.assertEqual(embedding.shape, (2, sentence_embeddings.LABSE_SIZE))

    def test_laser_embedding_size(self):
        sentences = ["This is a test sentence", "Another one"]
        embedding = sentence_embeddings.embedd_sentences(
            sentences, sentence_embeddings.EmbeddingType.LASER
        )
        self.assertEqual(embedding.shape, (2, sentence_embeddings.LASER_SIZE))

    def test_laser_single_sentence(self):
        sentence = "This is a test sentence"
        embedding = sentence_embeddings.embedd_sentences(
            sentence, sentence_embeddings.EmbeddingType.LASER
        )
        self.assertEqual(embedding.shape, (1, sentence_embeddings.LASER_SIZE))

    def test_labse_single_sentence(self):
        sentence = "This is a test sentence"
        embedding = sentence_embeddings.embedd_sentences(
            sentence, sentence_embeddings.EmbeddingType.LABSE
        )
        self.assertEqual(embedding.shape, (1, sentence_embeddings.LABSE_SIZE))


if __name__ == "__main__":
    unittest.main()
