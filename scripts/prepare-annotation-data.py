"""Loads data for annotation to database"""
import typer
from mars.db import collections, db_fields

EMBEDDING_TYPES = ["labse", "laser"]


def prepare_annotation_data(query_sentence: str):
    """Loads collection with annotation data"""
    for doc in collections.processed_texts.fetchAll(batchSize=1):
        print("Processing", doc["_id"], "...")
        processed_text_id = doc["_id"]
        text_id = doc[db_fields.TEXT_ID]
        for sent_num, sentence in enumerate(doc[db_fields.SENTENCES]):
            new_doc = collections.annotations.createDocument()
            new_doc[db_fields.PROCESSED_TEXT_ID] = processed_text_id
            new_doc[db_fields.TEXT_ID] = text_id
            new_doc[db_fields.SENTENCE] = sentence
            new_doc[db_fields.SENT_NUM] = sent_num
            new_doc[db_fields.QUERY_TARGET] = query_sentence
            embeddings = dict()
            for emb_type in EMBEDDING_TYPES:
                try:
                    embeddings[emb_type] = doc[emb_type]
                except KeyError:
                    print("No embedding type", emb_type, "found.")

            new_doc[db_fields.EMBEDDINGS] = embeddings
            new_doc.save()
        # print("Processed", sent_num + 1, "sentences.")


if __name__ == "__main__":
    typer.run(prepare_annotation_data)
