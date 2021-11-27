"""Splits all text to sentences and saves them to db"""
from mars.sentences_splitting import split_to_sentences
import mars

if __name__ == "__main__":

    min_key_query = f"FOR u IN {mars.db.collections.DOCUMENTS} SORT TO_NUMBER(u._key) ASC LIMIT 1 RETURN TO_NUMBER(u._key)"
    max_key_query = f"FOR u IN {mars.db.collections.DOCUMENTS} SORT TO_NUMBER(u._key) DESC LIMIT 1 RETURN TO_NUMBER(u._key)"
    min_key = mars.db.database.AQLQuery(min_key_query, 1, rawResults=True)[0]
    max_key = mars.db.database.AQLQuery(max_key_query, 1, rawResults=True)[0]

    split_to_sentences(key_min=min_key, key_max=max_key)
