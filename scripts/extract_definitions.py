import mars
from mars.document_definition_scoring import document_definition_scoring

if __name__ == "__main__":
    # get lowest and highest key(id) numbers
    min_key_query = f"FOR u IN {mars.db.collections.DOCUMENTS} SORT TO_NUMBER(u._key) ASC LIMIT 1 RETURN TO_NUMBER(u._key)"
    max_key_query = f"FOR u IN {mars.db.collections.DOCUMENTS} SORT TO_NUMBER(u._key) DESC LIMIT 1 RETURN TO_NUMBER(u._key)"
    min_key = mars.db.database.AQLQuery(min_key_query, 1, rawResults=True)[0]
    max_key = mars.db.database.AQLQuery(max_key_query, 1, rawResults=True)[0]

    # pass it to dedicated definitions
    document_definition_scoring(min_key, max_key, path_to_model="distilbert-base-uncased")