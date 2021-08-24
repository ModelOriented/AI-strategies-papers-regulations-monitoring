"""Splits all text to sentences and saves them to db"""
import dotenv
import mars.db
import spacy
import typer
from mars.db import collections
from mars.db.db_fields import CONTENT, SENTENCES, SOURCE, TEXT_ID

dotenv.load_dotenv()

# en = spacy.load("en_core_web_sm")

query = f"""FOR x IN {collections.DOCUMENTS} FILTER x.{SOURCE} == @source 
FOR y IN {collections.CONTENTS} RETURN y"""


def split_to_sentences(source: str) -> None:
    """Splits all text to sentences and saves them to db"""
    if source == "all":
        documents = collections.contents.fetchAll()
    else:

        documents = collections.contents.fetchByExample({SOURCE: source}, batchSize=100)
        print("Processing documents from source:", source)
    for doc in documents:
        if (
            len(
                collections.processed_texts.fetchFirstExample(
                    {"textId": "Texts/" + doc["_key"]}
                )
            )
            == 0
        ):  # if the text is not already splitted
            print("Processing", doc["_key"], "...")
            text = doc[CONTENT]
            sents = [str(s) for s in list(en(text).sents)]
            processed_text_doc = collections.processed_texts.createDocument()
            processed_text_doc[TEXT_ID] = doc["_id"]
            processed_text_doc[SENTENCES] = sents
            processed_text_doc.save()
        else:
            print("Skipping", doc["_key"])


if __name__ == "__main__":
    typer.run(split_to_sentences)
