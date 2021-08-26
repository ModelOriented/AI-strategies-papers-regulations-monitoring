import streamlit as st

from mars.db import collections
from mars.db.db_fields import ANNOTATION_RESULT, QUERY_TARGET, SENTENCE
from mars.db.new_api import database

"""## Issue"""
issue = st.empty()
"""## Sentence"""
sentence_text = st.empty()
"""Does issue appear in sentence?"""

annotations = database[collections.ANNOTATIONS]

q = f"""FOR d IN Annotations
    FILTER d.{ANNOTATION_RESULT} == null
    SORT d.score DESC
    LIMIT 1
    RETURN d"""


def get_new_item():
    print("Getting new item")
    doc = database.aql.execute(q).next()
    sentence_text.write(doc[SENTENCE])
    issue.write(doc[QUERY_TARGET])
    return doc


def save_annotation(doc, result: bool):
    doc[ANNOTATION_RESULT] = result
    annotations.update(doc)


d = get_new_item()


if st.button("Yes"):
    save_annotation(d, True)
if st.button("No"):
    save_annotation(d, False)

# TODO: cofanie anotacji
