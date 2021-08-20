from enum import Enum
from mars.db import collections
from mars.db import db_fields
import random 
EMBEDDING_TYPES = ["labse", "laser"]

class ScoreingStrategy(str,Enum):
    random = "random"


def prepare_annotation_data():
    """Loads collection with annotation data"""
    for doc in collections.processed_texts.fetchAll():
        print("Processing", doc['_id'], '...')
        processed_text_id = doc["_id"]
        text_id = doc[db_fields.TEXT_ID]
        for sent_num, sentence in enumerate(doc[db_fields.SENTENCES]):
            new_doc = collections.annotations.createDocument()
            new_doc[db_fields.PROCESSED_TEXT_ID]=processed_text_id
            new_doc[db_fields.TEXT_ID]=text_id
            new_doc[db_fields.SENTENCE]=sentence
            new_doc[db_fields.SENT_NUM]=sent_num
            embeddings = dict()
            for emb_type in EMBEDDING_TYPES:
                try:
                    embeddings[emb_type] = doc[emb_type]
                except KeyError as e:
                    print(e)
                
            new_doc[db_fields.EMBEDDINGS] = embeddings
            new_doc.save()
        

def score_all(strategy: ScoreingStrategy):
    if strategy==ScoreingStrategy.random:
        for doc in collections.annotations.fetchAll():
            doc[db_fields.SENTENCE_SAMPLING_SCORE] = random.random()
    else:
        ...# TODO: throw err
