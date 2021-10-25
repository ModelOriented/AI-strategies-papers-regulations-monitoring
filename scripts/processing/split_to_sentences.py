"""Splits all text to sentences and saves them to db"""
from typing import List

import mars.db
import spacy
import typer
from mars.db import collections
from mars import logging
from mars.db.db_fields import (
    CONTENT,
    DOC_ID,
    SEGMENT_ID,
    FILENAME,
    ID,
    SOURCE,
    URL,
    id_to_key,
    SENTENCE,
    HTML_TAG,
    IS_HEADER,
    SEQUENCE_NUMBER,
    SENTENCE_NUMBER,
)

BATCH_SIZE = 100

en = spacy.load("en_core_web_sm")

query = f"""FOR x IN {collections.DOCUMENTS} FILTER x.{SOURCE} == @source
FOR y IN {collections.SEGMENTED_TEXTS} FILTER x._id == y.{DOC_ID} RETURN y"""

logger = logging.new_logger(__name__)

def split_text(text: str) -> List[str]:
    while "\n\n" in text:
        text = text.replace("\n\n", "\n")
    sents = []

    for s in list(en(text).sents):
        a = str(s)
        a = a.replace("\n", " ")
        if not (a == " " or a == ""):
            sents.append(a)
    return sents


def split_to_sentences(source: str) -> None:
    """Splits all text to sentences and saves them to db"""
    if source == "all":
        segments = collections.segmented_texts.fetchAll()
    else:
        print("Processing documents from source:", source)
        segments = mars.db.database.AQLQuery(
            query, BATCH_SIZE, bindVars={"source": source}
        )

    get_done_query = f"FOR u IN {collections.SENTENCES} RETURN u.{SEGMENT_ID}"
    done_segments = mars.db.database.AQLQuery(get_done_query, 10000, rawResults=True)
    done_segments = set(list(done_segments))

    all_segments = list(segments)
    todo_segments = [segment for segment in all_segments if segment[ID] not in done_segments]

    logger.info(
        "Already splited documents: %s / %s"
        % (len(all_segments) - len(todo_segments), len(all_segments))
    )
    logger.info(
        "Waiting for split documents: %s / %s" % (len(todo_segments), len(all_segments))
    )

    for index, segment in enumerate(todo_segments):
        logger.info(
            "Processing %s (%s%%)"
            % (segment[ID], round(100 * index / len(todo_segments), 1))
        )
        text = segment[CONTENT]
        sents = split_text(text)
        counter = 0
        for sent in sents:
            sentence = collections.sentences.createDocument()
            # indicate order of sentences in segment
            sentence[SENTENCE_NUMBER] = counter
            sentence[HTML_TAG] = segment[HTML_TAG]
            sentence[IS_HEADER] = segment[IS_HEADER]
            # indicate order of segments in doc
            sentence[SEQUENCE_NUMBER] = segment[SEQUENCE_NUMBER]
            sentence[SEGMENT_ID] = segment[ID]
            sentence[DOC_ID] = segment[DOC_ID]
            sentence[SENTENCE] = sent
            sentence.save()
            counter += 1

if __name__ == "__main__":
    typer.run(split_to_sentences)
