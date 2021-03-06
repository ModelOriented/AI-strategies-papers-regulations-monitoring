import os
import uuid
from itertools import accumulate, repeat, takewhile
from typing import Iterator, List

from mars import config
from mars.db import collections
from mars.db._connection import database
from mars.db.db_fields import (
    CONTENT,
    DOC_ID,
    DOC_JOBS,
    DOC_NAME,
    EXTRACTION_METHOD,
    FILE_TYPE,
    FILENAME,
    SOURCE,
    URL,
    USER,
    ExtractionMethod,
    FileType,
    SourceWebsite,
)
from mars.storage import FileSync

env_user = config.user
document_source_field_keys = [URL, FILENAME, FILE_TYPE, SOURCE]


def is_document_present(url: str) -> bool:
    """Checks if documents from given url is downloaded"""
    return len(collections.document_sources.fetchFirstExample({URL: url})) == 1


def is_content_present(url: str, method: ExtractionMethod) -> bool:
    """Checks if documents from given url is downloaded"""
    return (
        len(
            collections.contents.fetchFirstExample(
                {URL: url, EXTRACTION_METHOD: method}
            )
        )
        == 1
    )


def save_doc(
    url: str,
    raw_file_content,
    file_type: FileType,
    source: SourceWebsite,
    additional_data: dict = dict(),
    user: str = env_user,
    name: str = None,
) -> None:
    """Saves new source document to database"""
    file_name = _new_file(raw_file_content, file_type)
    doc = collections.document_sources.createDocument()
    doc[URL] = url
    doc[FILE_TYPE] = file_type
    doc[FILENAME] = file_name
    doc[SOURCE] = source
    doc[USER] = user
    doc[DOC_NAME] = name
    doc[DOC_JOBS] = []
    for key, value in additional_data.items():
        if key not in document_source_field_keys:
            doc[key] = value
    doc.save()
    return doc


def save_extracted_content(
    source_url: str, content: str, extraction_method: ExtractionMethod
) -> None:
    """Saves extracted content to database"""
    doc = collections.contents.createDocument()
    try:
        id = collections.document_sources.fetchFirstExample({URL: source_url})[0]._id
    except IndexError:
        raise Exception("Document not found")

    doc[DOC_ID] = id
    doc[CONTENT] = content
    doc[EXTRACTION_METHOD] = extraction_method
    doc[URL] = source_url
    doc[USER] = env_user
    doc.save()


def _new_file(file_content, file_type: str):
    """
    for pdfs file_content is current filename
    @TODO rename
    """
    file_id = str(uuid.uuid4()) + "." + file_type
    with FileSync(file_id) as filename:
        if file_content is None:
            with open(filename, "w") as file:
                file.write("")
        elif file_type == FileType.pdf:
            with open(filename, "wb") as file:
                file.write(file_content)
        else:
            with open(filename, "w") as file:
                file.write(file_content)
    return file_id


def fetch_batches_until_empty(query, batch_size=1000, **kwargs) -> Iterator[list]:
    """Fetch collection in batches. Stop fetching when there is no fields after filtering"""
    generator = (list(database.AQLQuery(query, **kwargs)) for i in repeat(1))
    return takewhile(lambda x: len(x) != 0, generator)


def get_ids_of_docs_between(key_min: int, key_max: int) -> List[str]:
    """Returns list of ids of documents between given keys"""
    all_docs_between_keys = f"""FOR d in {collections.DOCUMENTS}
    FILTER TO_NUMBER(d._key)>={key_min} && TO_NUMBER(d._key)<={key_max}
    return d._id"""
    todo_docs = database.AQLQuery(all_docs_between_keys, 10000, rawResults=True)
    return todo_docs
