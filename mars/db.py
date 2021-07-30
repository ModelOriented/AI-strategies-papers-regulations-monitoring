import os
import uuid
from enum import Enum

from pyArango.connection import Connection
from dotenv import load_dotenv

load_dotenv()

URL = "url"
FILENAME = "filename"
FILE_TYPE = "file_type"
SOURCE = "source_website"
DOC_ID = "source_doc_id"
CONTENT = "content"
EXTRACTION_METHOD = "extraction_method"


class SourceWebsite(str, Enum):
    oecd = "oecd"
    localhost = "localhost"


class FileType(str, Enum):
    pdf = "pdf"
    html = "html"


class ExtractionMetod(str, Enum):
    newspaper = "newspaper3k"
    dragnet = "dragnet"
    pdfminer = "pdfminer"


def get_collection_or_create(db, collection_name: str):
    try:
        return db[collection_name]
    except KeyError:
        return db.createCollection(name=collection_name)


conn = Connection(
    username=os.getenv("ARANGODB_USERNAME"),
    password=os.getenv("ARANGODB_PASSWORD"),
    arangoURL=os.getenv("ARANGODB_URL"),
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


def is_content_present(url: str, method: ExtractionMetod) -> bool:
    """Checks if documents from given url is downloaded"""
    return len(contents.fetchFirstExample({URL: url, EXTRACTION_METHOD: method})) == 1


def save_doc(
    url: str, raw_file_content, file_type: FileType, source: SourceWebsite
) -> None:
    """Saves new source document to database"""
    file_name = _new_file(raw_file_content, file_type)
    doc = documentSources.createDocument()

    doc[URL] = url
    doc[FILE_TYPE] = file_type
    doc[FILENAME] = file_name
    doc[SOURCE] = source
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
    doc[EXTRACTION_METHOD] = extraction_method
    doc[URL] = source_url
    doc.save()


def _new_file(file_content, file_type: str):
    """
    for pdfs file_content is current filename
    @TODO rename
    """
    filename = os.path.join(
        os.getenv("RAW_FILES_DIR"), str(uuid.uuid4()) + "." + file_type
    )
    if file_type == FileType.pdf:
        with open(filename, "wb") as file:
            file.write(file_content)
    else:
        with open(filename, "w") as file:
            file.write(file_content)
    return filename
