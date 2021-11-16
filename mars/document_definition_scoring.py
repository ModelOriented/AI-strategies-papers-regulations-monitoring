import mars.db
from mars import logging
from mars.db import collections
from mars.db.db_fields import (
    SENTENCE,
    SENTENCE_DOC_ID,
    IS_DEFINITION
)
from mars.definition_model import DistilBertBaseUncased

BATCH_SIZE = 100

logger = logging.new_logger(__name__)


def document_definition_scoring(key_min: int, key_max: int,
                                path_to_model: str = "../models/distilbert-base-uncased") -> None:
    """

    @param key_min: lowest document key
    @param key_max: highest document key
    @param path_to_model: path to folder with definition model
    """

    # get documents with at least one not-scored sentence
    get_todo_docs_query = f"FOR u IN {collections.SENTENCES} " \
                          f"FILTER TO_NUMBER(SPLIT(u.{SENTENCE_DOC_ID}, \"/\")[1]) >= {key_min} " \
                          f"&& TO_NUMBER(SPLIT(u.{SENTENCE_DOC_ID}, \"/\")[1]) <= {key_max}" \
                          f"FILTER IS_NUMBER(u.{IS_DEFINITION}) == false " \
                          f"RETURN DISTINCT u.{SENTENCE_DOC_ID} "

    todo_docs = mars.db.database.AQLQuery(get_todo_docs_query, 10000, rawResults=True)
    todo_docs = set(list(todo_docs))

    dbc = DistilBertBaseUncased(path_to_model)

    for doc_index, doc in enumerate(todo_docs):

        # get all not-scored sentences
        get_todo_sentences_query = f"FOR u IN {collections.SENTENCES} " \
                                   f"FILTER u.{SENTENCE_DOC_ID} == \"{doc}\" " \
                                   f"FILTER IS_NUMBER(u.{IS_DEFINITION}) == false " \ 
                                   f"RETURN DISTINCT u"

        todo_sentences = mars.db.database.AQLQuery(get_todo_sentences_query, 10000)

        logger.info(
            "(%s%%) Processing document %s. Sentences count: %s"
            % (round(100 * doc_index / len(todo_docs), 1), doc, len(list(todo_sentences)))
        )

        for index, sentence in enumerate(todo_sentences):
            text = sentence[SENTENCE]
            sentence[IS_DEFINITION] = float(dbc.predict_single_sentence(text))
            sentence.save()
