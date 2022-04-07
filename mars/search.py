from mars import db

escape = str.maketrans({'"': '\\"'})


def prepare_for_search(text: str) -> str:
    text = text.replace("\\", " ").strip()
    text = text.translate(escape)
    return text


def search_for(text: str) -> list:
    text = prepare_for_search(text)
    query = f"""FOR doc in sentences_view SEARCH ANALYZER(PHRASE(doc.sentence, "{text}"), "text_en")
    RETURN doc"""
    q = db.database.AQLQuery(query, rawResults=True)
    return [{"search_query": text, **d} for d in q]
