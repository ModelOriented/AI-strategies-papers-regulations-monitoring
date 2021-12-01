from collections import defaultdict
from typing import Dict, List, Union

import numpy as np
from tqdm import tqdm

from mars import db, logging, sentence_embeddings
from mars.db import collections, db_fields
from mars.db.db import fetch_batches_until_empty

logger = logging.new_logger(__name__)


def similarity(sent_embedding: np.ndarray, query_embedding: np.ndarray) -> float:
    """Calculates similarity between single sentence and query (both provided as embeddings)."""
    return np.matmul(np.array(sent_embedding), np.transpose(query_embedding))


def calculate_similarities_to_targets(
    queries: List[str],
    emb_type: db_fields.EmbeddingType = db_fields.EmbeddingType.LABSE,
    key=db_fields.FILENAME,
    filter=dict(),
) -> Dict[str, Dict[str, float]]:
    """Calculate similarities of all sentences from all processed texts to given targets.
    Returns mapping: filename (or other choosen key) -> query -> list of similarities of all sentences in text.
    Filters processed texts using filter parameter."""
    target_embeddings = sentence_embeddings.get_sentence_to_embedding_mapping(
        queries, emb_type
    )
    all_similarities = defaultdict(dict)
    logging.debug("Loading targets similarities...")
    for processed_text in tqdm(
        db.collections.processed_texts.fetchByExample(filter, batchSize=100)
    ):
        if (
            processed_text[db_fields.EMBEDDINGS] is None
            or processed_text[db_fields.EMBEDDINGS][emb_type] is None
        ):
            logging.error(
                f"Missing sentences embedding ({emb_type}) in {processed_text['_id']}"
            )
        else:
            for (target_sentence, target_embedding) in target_embeddings.items():
                try:
                    scores = list(
                        np.matmul(
                            np.array(processed_text[db_fields.EMBEDDINGS][emb_type]),
                            np.transpose(target_embedding),
                        )
                    )
                    all_similarities[processed_text[key]][target_sentence] = scores
                except Exception as e:
                    logging.exception(e)
    all_similarities = dict(all_similarities)

    return all_similarities


class SimilarityCalculator:
    def __init__(self, emb_type: db_fields.EmbeddingType):
        self.target_embeddings = dict()
        self.emb_type = emb_type

    def calc_and_save_similarity_for_sentence(
        self, sent, target: Union[List[str], str]
    ):
        if sent[db_fields.ISSUES] is None:
            sent[db_fields.ISSUES] = dict()
        if sent[db_fields.ISSUES][self.emb_type] is None:
            sent[db_fields.ISSUES][self.emb_type] = dict()
        if type(target) == list:
            for t in target:
                self._single_target_calc_and_save_simmilarity(sent, t)
        else:
            self._single_target_calc_and_save_simmilarity(sent, target)
        sent.forceSave()

    def calc_similarity(self, sent_embedding: np.array, target: str):
        try:
            target_embedding = self.target_embeddings[target]
        except KeyError:
            target_embedding = sentence_embeddings.embedd_sentences(
                target, self.emb_type
            )[0]
            self.target_embeddings[target] = target_embedding
        return similarity(sent_embedding, target_embedding)

    def _single_target_calc_and_save_simmilarity(self, sent, target: str):
        sent_embedding = sent[db_fields.EMBEDDING][self.emb_type]
        similarity = self.calc_similarity(sent_embedding, target)
        sent[db_fields.ISSUES][self.emb_type][target] = similarity


def infer_issues_for_documents(
    key_min: int,
    key_max: int,
    issue_search_method: db_fields.IssueSearchMethod,
    issues: List[str],
):
    todo_docs = db.get_ids_of_docs_between(key_min, key_max)
    simmilarity_calculator = SimilarityCalculator(issue_search_method)
    for doc_key in todo_docs:
        for sents_docs in fetch_batches_until_empty(
            f'FOR u IN {collections.SENTENCES} FILTER u.{db_fields.SENTENCE_DOC_ID} == "{doc_key}" && (u.{db_fields.ISSUES}.{issue_search_method} == NULL) LIMIT 100 RETURN u'
        ):
            logger.info(
                f"Processing doc {doc_key}. Sentences to process in this batch: {len(sents_docs)}"
            )
            for sent in sents_docs:
                simmilarity_calculator.calc_and_save_similarity_for_sentence(
                    sent, issues
                )


def clean_issues(
    key_min: int, key_max: int, issue_search_method: db_fields.IssueSearchMethod
):
    """Removes all issues from given method from documents between key_min and key_max."""
    todo_docs = db.get_ids_of_docs_between(key_min, key_max)
    for doc_key in todo_docs:
        for sents_docs in fetch_batches_until_empty(
            f'FOR u IN {collections.SENTENCES} FILTER u.{db_fields.SENTENCE_DOC_ID} == "{doc_key}" && u.{db_fields.ISSUES}.{issue_search_method} != NULL LIMIT 100 RETURN u'
        ):
            for sent in sents_docs:
                del sent[db_fields.ISSUES]
                sent.forceSave()
