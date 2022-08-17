from typing import List

import spacy
from mair import logging


BATCH_SIZE = 100

en = spacy.load("en_core_web_sm")

logger = logging.new_logger(__name__)


def split_text(text: str) -> List[str]:
    """
    Splits texts into senteces
    @param text: str
    @return: List of strings
    """
    while "\n\n" in text:
        text = text.replace("\n\n", "\n")
    sents = []

    for s in list(en(text).sents):
        a = str(s)
        a = a.replace("\n", " ")
        if not (a == " " or a == ""):
            sents.append(a)
    return sents