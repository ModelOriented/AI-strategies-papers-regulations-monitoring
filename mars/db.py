import os
import uuid
from enum import Enum

from pyArango.connection import Connection

URL = "url"
FILENAME = "filename"
FILE_TYPE = "file_type"
SOURCE = "source_website"
DOC_ID = "source_doc_id"
CONTENT = "content"
EXTRACTION_METHOD = "extraction_method"

FILES_DIR = "raw_data"


class SourceWebsite(str, Enum):
    oecd = "oecd"


class FileType(str, Enum):
    pdf = "pdf"
    html = "html"


class ExtractionMetod(str, Enum):
    newspaper = "newspaper3k"
    dragnet = "dragnet"


def get_collection_or_create(db, collection_name: str):
    try:
        return db[collection_name]
    except KeyError:
        return db.createCollection(name=collection_name)


conn = Connection(
    username="root", password="rootpassword", arangoURL="http://127.0.0.1:8080"
)
try:
    db = conn.databases["mars"]
except KeyError:
    db = conn.createDatabase("mars")

documentSources = get_collection_or_create(db, "Documents")
contents = get_collection_or_create(db, "Texts")


def is_document_present(url: str) -> bool:
    """Checks if documents from given url is downloaded"""
    return len(documentSources.fetchFirstExample({URL: url})) == 1


def save_doc(
    url: str, raw_file_content, file_type: FileType, source: SourceWebsite
) -> None:
    """Saves new source document to database"""
    file_name = _new_file(file_type, raw_file_content)
    doc = documentSources.createDocument()

    doc[URL] = url
    doc[FILE_TYPE] = str(file_type)
    doc[FILENAME] = file_name
    doc[SOURCE] = str(source)
    doc.save()


def save_extracted_content(
    source_url: str, content: str, extraction_method: ExtractionMetod
) -> None:
    """Saves extracted content to database"""
    doc = contents.createDocument()
    try:
        id = documentSources.fetchFirstExample({URL: source_url})[0]._id
    except IndexError:
        raise Exception("Document not found")

    doc[DOC_ID] = id
    doc[CONTENT] = content
    doc[EXTRACTION_METHOD] = str(extraction_method)
    doc.save()


def _new_file(file_content, file_type: str):
    filename = str(uuid.uuid4()) + file_type
    with open(os.path.join(FILES_DIR, filename), "w") as file:
        file.write(file_content)
    return filename
