import unittest
import os
from mars import config
from mars.definition_model import DistilBertBaseUncased

path_to_model = config.models_dir + "/" + 'distilbert-base-uncased'
path_to_tokenizer = config.models_dir + '/tokenizers/' + 'distilbert-base-uncased'
dbu = DistilBertBaseUncased(path_to_model, path_to_tokenizer)


class DefinitionScoring(unittest.TestCase):

    def test_score_definition(self):
        text = "Jellyfish and sea jellies are the informal common names given to the medusa-phase of certain gelatinous members of the subphylum Medusozoa, a major part of the phylum Cnidaria"
        score = dbu.predict_single_sentence(text)
        self.assertIsInstance(score, float)
        self.assertTrue(score > 0.5)

    def test_long_test(self):
        with open("long_text.txt", 'r') as f:
            text = f.readlines()
            text = "\n".join(text).replace("\n", "")

        score = dbu.predict_single_sentence(text)
        self.assertIsInstance(score, float)

if __name__ == "__main__":
    unittest.main()
