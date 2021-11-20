import unittest
import os
from mars import config
from mars.definition_model import DistilBertBaseUncased

path_to_model = '../models/' + 'distilbert-base-uncased'
dbu = DistilBertBaseUncased(path_to_model)


class DefinitionScoring(unittest.TestCase):

    def test_score_definition(self):
        text = "Jellyfish and sea jellies are the informal common names given to the medusa-phase of certain gelatinous members of the subphylum Medusozoa, a major part of the phylum Cnidaria"
        score = dbu.predict_single_sentence(text)
        self.assertIsInstance(score, float)
        self.assertTrue(score > 0.5)

    def test_long_test(self):
        text = "Participating insurance and reinsurance undertakings, insurance holding companies and mixed financial holding companies which, for the calculation of group solvency, use method 1 as defined in Article 230 of Directive 2009/138/EC, either exclusively or in combination with method 2 as defined in Article 233 of that Directive, shall submit annually the information referred to in Article 304(1)(d) of Delegated Regulation (EU) 2015/35, in conjunction with Article 372(1) of that Delegated Regulation, using the following templates in relation to all material ring-fenced funds and all material matching adjustment portfolios related to the part that is consolidated as referred to in points (a) and (c) of Article 335(1) of Delegated Regulation (EU) 2015/35, as well as in relation to the remaining part:"
        score = dbu.predict_single_sentence(text)
        self.assertIsInstance(score, float)

if __name__ == "__main__":
    unittest.main()
