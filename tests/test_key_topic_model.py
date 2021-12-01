import unittest

from mars.keyword_topic_model import KeywordTopicModel

test_sentences = [
    'Our results reveal a global convergence emerging around five ethical principles (transparency, justice and fairness, non-maleficence, responsibility and privacy), with substantive divergence in relation to how these principles are interpreted, why they are deemed important, what issue, domain or actors they pertain to, and how they should be implemented.',
    'The trust in machine learning is proportional to the extent of interpretability',
    'nothing is here',
    'Transparency Justice, fairness, and equity Non-maleficence Responsibility and accountability Privacy Beneficence Freedom and autonomy Trust Sustainability Dignity Solidarity'
]

class KeyTopicModel(unittest.TestCase):
    def test_score_topics(self):
        ktm = KeywordTopicModel()

        pred1 = ktm.score_sentence(test_sentences[0])
        pred2 = ktm.score_sentence(test_sentences[1])
        pred3 = ktm.score_sentence(test_sentences[2])
        pred4 = ktm.score_sentence(test_sentences[3])

        expected1 = dict.fromkeys(ktm.topics.keys(), 0)
        expected1['Transparency'], expected1['Justice, fairness, and equity'], expected1['Non-maleficence'], expected1[
            'Responsibility and accountability'], expected1['Privacy'] = 1, 1, 1, 1, 1

        expected2 = dict.fromkeys(ktm.topics.keys(), 0)
        expected2['Trust'], expected2['Transparency'] = 1, 1

        expected3 = dict.fromkeys(ktm.topics.keys(), 0)
        expected4 = dict.fromkeys(ktm.topics.keys(), 1)

        self.assertEqual(pred1, expected1)
        self.assertEqual(pred2, expected2)
        self.assertEqual(pred3, expected3)
        self.assertEqual(pred4, expected4)
