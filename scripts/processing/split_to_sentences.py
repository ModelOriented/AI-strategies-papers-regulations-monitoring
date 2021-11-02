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

    get_done_docs_query = f"FOR u IN {collections.SENTENCES} RETURN DISTINCT u.{DOC_ID}"
    done_docs = mars.db.database.AQLQuery(get_done_docs_query, 10000, rawResults=True)
    done_docs = set(list(done_docs))

    if source == "all":
        get_all_docs_query = f"FOR u IN {collections.DOCUMENTS} RETURN DISTINCT u.{ID}"
        all_docs = mars.db.database.AQLQuery(get_all_docs_query, 10000, rawResults=True)
    else:
        print("Processing documents from source:", source)
        get_all_docs_query = f"FOR u IN {collections.DOCUMENTS} FILTER y.{SOURCE} == @source RETURN DISTINCT u.{ID}"
        all_docs = mars.db.database.AQLQuery(
            get_all_docs_query, 10000, rawResults=True, bindVars={"source": "source"}
        )

    all_docs = set(list(all_docs))
    todo_docs = list(all_docs - done_docs)

    for doc_index, doc in enumerate(todo_docs):
        get_all_segments_query = f"FOR u IN {collections.SEGMENTED_TEXTS} FILTER u.{DOC_ID} == @docid RETURN u"
        all_segments = mars.db.database.AQLQuery(
            get_all_segments_query, 10000, bindVars={"docid": doc}
        )

        get_done_query = f"FOR u IN {collections.SENTENCES} FILTER u.{DOC_ID} == @docid RETURN DISTINCT u.{SEGMENT_ID}"
        done_segments = mars.db.database.AQLQuery(
            get_done_query, 10000, rawResults=True, bindVars={"docid": doc}
        )
        done_segments = set(list(done_segments))

        all_segments = list(all_segments)
        todo_segments = [
            segment for segment in all_segments if segment[ID] not in done_segments
        ]

        logger.info(
            "(%s%%) Processing document %s. Segments count: %s"
            % (round(100 * doc_index / len(todo_docs), 1), doc, len(todo_segments))
        )

        for index, segment in enumerate(todo_segments):
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
