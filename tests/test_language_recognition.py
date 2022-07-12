import unittest

from mair import language_recognition

EN_SENTENCE = "This is a test sentence."
FR_SENTENCE = "Ceci est une phrase de test."
DE_SENTENCE = "Das ist ein Testtext."
RU_SENTENCE = "Это проверочная фраза."
PL_SENTENCE = "To jest testowa fraza."
UNKNOWN_SENTENCE = "....,...,111."


class LanguageRecognition(unittest.TestCase):
    def test_recognize_en(self):
        lang = language_recognition.detect_language(EN_SENTENCE)
        self.assertEqual(lang, "en")

    def test_recognize_fr(self):
        lang = language_recognition.detect_language(FR_SENTENCE)
        self.assertEqual(lang, "fr")

    def test_recognize_de(self):
        lang = language_recognition.detect_language(DE_SENTENCE)
        self.assertEqual(lang, "de")

    def test_recognize_ru(self):
        lang = language_recognition.detect_language(RU_SENTENCE)
        self.assertEqual(lang, "ru")

    def test_recognize_pl(self):
        lang = language_recognition.detect_language(PL_SENTENCE)
        self.assertEqual(lang, "pl")

    def test_recognize_unknown(self):
        lang = language_recognition.detect_language(UNKNOWN_SENTENCE)
        self.assertEqual(lang, "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
