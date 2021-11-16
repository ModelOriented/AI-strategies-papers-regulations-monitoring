import mars.db
from mars import logging
from mars.db import collections
from mars.db.db_fields import (
    ID,
    SENTENCE,
    SENTENCE_DOC_ID,
    IS_DEFINITION
)
from mars.definition_model import DistilBertBaseUncased
from typing import List
BATCH_SIZE = 100

logger = logging.new_logger(__name__)


def get_docs_range(key_min: int, key_max: int) -> List[str]:
    res = []
    for i in range(key_min, key_max + 1):
        res.append("Documents/" + str(i))
    return res


def document_definition_scoring(key_min: int, key_max: int, path_to_model:str = "../models/distilbert-base-uncased") -> None:
    """

    @param key_min: lowest document key
    @param key_max: highest document key
    @param path_to_model: path to folder with definition model
    """
    get_docs_query = f"FOR u IN {collections.DOCUMENTS} " \
                     f"FILTER TO_NUMBER(u._key) >= {key_min} && TO_NUMBER(u._key) <= {key_max}" \
                     f" RETURN DISTINCT u.{ID}"

    all_docs = mars.db.database.AQLQuery(get_docs_query, 10000, rawResults=True)

    # get all sentences from docs
    get_done_docs_query = f"FOR u IN {collections.SENTENCES} " \
                          f"FILTER u.{SENTENCE_DOC_ID} IN TO_ARRAY({get_docs_range(key_min, key_max)}) " \
                          f"FILTER IS_NUMBER(u.{IS_DEFINITION}) == true " \
                          f"FILTER u.{IS_DEFINITION} >= 0 " \
                          f"RETURN DISTINCT u.{SENTENCE_DOC_ID} "

    done_docs = mars.db.database.AQLQuery(get_done_docs_query, 10000, rawResults=True)
    done_docs = set(list(done_docs))

    all_docs = set(list(all_docs))
    todo_docs = list(all_docs - done_docs)

    dbc = DistilBertBaseUncased(path_to_model)

    for doc_index, doc in enumerate(todo_docs):

        get_all_sentences_query = f"FOR u IN {collections.SENTENCES} FILTER u.{SENTENCE_DOC_ID} == \"{doc}\" RETURN u"
        all_sentences = mars.db.database.AQLQuery(
            get_all_sentences_query, 10000
        )

        get_done_sentences_query = f"FOR u IN {collections.SENTENCES} " \
                                   f"FILTER u.{SENTENCE_DOC_ID} == \"{doc}\" " \
                                   f"FILTER IS_NUMBER(u.{IS_DEFINITION}) == true " \
                                   f"FILTER u.{IS_DEFINITION} >= 0 " \
                                   f"RETURN DISTINCT u._key "

        done_sentences = mars.db.database.AQLQuery(
            get_done_sentences_query, 10000, rawResults=True
        )
        done_sentences = set(list(done_sentences))

        all_sentences = list(all_sentences)
        todo_sentences = [
            sentence for sentence in all_sentences if sentence[ID] not in done_sentences
        ]

        logger.info(
            "(%s%%) Processing document %s. Sentences count: %s"
            % (round(100 * doc_index / len(todo_docs), 1), doc, len(todo_sentences))
        )

        for index, sentence in enumerate(todo_sentences):
            text = sentence[SENTENCE]
            counter = 0
            sentence[IS_DEFINITION] = float(dbc.predict_single_sentence(text))
            sentence.save()
            counter += 1
