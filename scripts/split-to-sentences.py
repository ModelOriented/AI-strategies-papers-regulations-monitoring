"""Splits all text to sentences and saves them to db"""
import dotenv
import spacy
from mars import db

dotenv.load_dotenv()

en = spacy.load("en_core_web_sm")

# split to sentences, only those texts that are not already splitted

for content in db.collections.contents.fetchAll():
    if (
        len(
            db.collections.processed_texts.fetchFirstExample(
                {"textId": "Texts/" + content["_key"]}
            )
        )
        == 0
    ):  # if the text is not already splitted
        print("Processing", content["_key"], "...")
        text = content["content"]
        sents = [str(s) for s in list(en(text).sents)]
        doc = db.collections.processed_texts.createDocument()
        doc["textId"] = content["_id"]
        doc["sentences"] = sents
        doc.save()
    else:
        print("Skipping", content["_key"])
