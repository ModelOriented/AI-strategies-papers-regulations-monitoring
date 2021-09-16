"""Loads data for annotation to database"""
import typer
from mars.db import collections
from mars.db.db_fields import (
    EMBEDDINGS,
    ID,
    PROCESSED_TEXT_ID,
    QUERY_TARGET,
    SENT_NUM,
    SENTENCE,
    SENTENCES,
    TEXT_ID,
    EmbeddingType,
)
from mars.db.new_api import database
from tqdm import tqdm

EMBEDDING_TYPES = ["labse", "laser"]


def prepare_annotation_data(query_sentence: str):
    """Loads collection with annotation data"""
    for doc in database[collections.PROCESSED_TEXTS]:
        print("Processing", doc[ID], "...")
        processed_text_id = doc[ID]
        text_id = doc[TEXT_ID]
        with database.begin_batch_execution() as batch_db:
            for sent_num, sentence in tqdm(enumerate(doc[SENTENCES])):
                new_doc = dict()
                new_doc[PROCESSED_TEXT_ID] = processed_text_id
                new_doc[TEXT_ID] = text_id
                new_doc[SENTENCE] = sentence
                new_doc[SENT_NUM] = sent_num
                new_doc[QUERY_TARGET] = query_sentence
                embeddings = dict()
                for emb_type in EmbeddingType:
                    try:
                        embeddings[emb_type] = doc[EMBEDDINGS][emb_type][sent_num]
                    except KeyError:
                        ...
                new_doc[EMBEDDINGS] = embeddings
                batch_db[collections.ANNOTATIONS].insert(new_doc)


if __name__ == "__main__":
    typer.run(prepare_annotation_data)
