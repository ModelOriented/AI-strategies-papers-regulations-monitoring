import spacy
from spacy.language import Language
from spacy_langdetect import LanguageDetector


@Language.factory("language_detector")
def _create_language_detector(nlp: Language, name: str) -> LanguageDetector:
    return LanguageDetector(language_detection_function=None)


nlp = spacy.load("en_core_web_sm")
nlp.add_pipe("language_detector")


def detect_language(text: str) -> str:
    """ "For given text returns ISO 639-1 Code of the language."""
    doc = nlp(text)
    return doc._.language["language"]
