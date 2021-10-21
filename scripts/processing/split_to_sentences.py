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
        documents = collections.segmented_texts.fetchAll()
    else:
        print("Processing documents from source:", source)
        documents = mars.db.database.AQLQuery(
            query, BATCH_SIZE, bindVars={"source": source}
        )

    get_done_query = f"FOR u IN {collections.SENTENCES} RETURN u.{DOC_ID}"
    done_docs = mars.db.database.AQLQuery(get_done_query, 10000, rawResults=True)
    done_docs = set(list(done_docs))

    all_docs = list(documents)
    todo_docs = [doc for doc in all_docs if doc[ID] not in done_docs]

    logger.info(
        "Already splited documents: %s / %s"
        % (len(all_docs) - len(todo_docs), len(all_docs))
    )
    logger.info(
        "Waiting for split documents: %s / %s" % (len(todo_docs), len(all_docs))
    )

    for index, doc in enumerate(todo_docs):
        logger.info(
            "Processing %s (%s%%)"
            % (doc[ID], round(100 * index / len(todo_docs), 1))
        )
        doc_source = collections.document_sources[id_to_key(doc[DOC_ID])]
        text = doc[CONTENT]
        sents = split_text(text)
        counter = 0
        for sent in sents:
            processed_text_doc = collections.sentences.createDocument()
            # indicate order of sentences in segment
            processed_text_doc[SENTENCE_NUMBER] = counter
            processed_text_doc[HTML_TAG] = doc[HTML_TAG]
            processed_text_doc[IS_HEADER] = doc[IS_HEADER]
            # indicate order of segments in doc
            processed_text_doc[SEQUENCE_NUMBER] = doc[SEQUENCE_NUMBER]
            processed_text_doc[DOC_ID] = doc[ID]
            processed_text_doc[SENTENCE] = sent
            processed_text_doc[FILENAME] = doc_source[URL].split("/")[-1]
            processed_text_doc.save()
            counter += 1

if __name__ == "__main__":
    typer.run(split_to_sentences)
