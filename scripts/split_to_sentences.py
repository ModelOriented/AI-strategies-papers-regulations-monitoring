"""Splits all text to sentences and saves them to db"""
from typing import List

import mars.db
import spacy
import typer
from mars.db import collections
from mars.db.db_fields import (
    CONTENT,
    DOC_ID,
    FILENAME,
    ID,
    SENTENCES,
    SOURCE,
    TEXT_ID,
    URL,
    id_to_key,
)

BATCH_SIZE = 100

en = spacy.load("en_core_web_sm")

query = f"""FOR x IN {collections.DOCUMENTS} FILTER x.{SOURCE} == @source
FOR y IN {collections.CONTENTS} FILTER x._id == y.{DOC_ID} RETURN y"""


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
        documents = collections.contents.fetchAll()
    else:
        print("Processing documents from source:", source)
        documents = mars.db.database.AQLQuery(
            query, BATCH_SIZE, bindVars={"source": source}
        )
        # documents = collections.contents.fetchByExample({SOURCE: source}, batchSize=100)
    for doc in documents:
        if (
            len(collections.processed_texts.fetchFirstExample({TEXT_ID: doc[ID]})) == 0
        ):  # if the text is not already splitted
            print("Processing", doc[ID], "...")
            doc_source = collections.document_sources[id_to_key(doc[DOC_ID])]
            text = doc[CONTENT]
            sents = split_text(text)
            processed_text_doc = collections.processed_texts.createDocument()
            processed_text_doc[TEXT_ID] = doc[ID]
            processed_text_doc[SENTENCES] = sents
            processed_text_doc[FILENAME] = doc_source[URL].split("/")[-1]
            processed_text_doc.save()
        else:
            print("Skipping", doc[ID])


if __name__ == "__main__":
    typer.run(split_to_sentences)
