import os
import uuid
from enum import Enum

from dotenv import load_dotenv
from mars.db import collections

load_dotenv()
os.makedirs(os.getenv("RAW_FILES_DIR"), exist_ok=True)


URL = "url"
FILENAME = "filename"
FILE_TYPE = "file_type"
SOURCE = "source_website"
DOC_ID = "source_doc_id"
CONTENT = "content"
EXTRACTION_METHOD = "extraction_method"

document_source_field_keys = [URL, FILENAME, FILE_TYPE, SOURCE]


class SourceWebsite(str, Enum):
    oecd = "oecd"
    manual = "manually_added"


class FileType(str, Enum):
    pdf = "pdf"
    html = "html"


class ExtractionMetod(str, Enum):
    newspaper = "newspaper3k"
    dragnet = "dragnet"
    pdfminer = "pdfminer"


def is_document_present(url: str) -> bool:
    """Checks if documents from given url is downloaded"""
    return len(collections.document_sources.fetchFirstExample({URL: url})) == 1


def is_content_present(url: str, method: ExtractionMetod) -> bool:
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
) -> None:
    """Saves new source document to database"""
    file_name = _new_file(raw_file_content, file_type)
    doc = collections.document_sources.createDocument()

    doc[URL] = url
    doc[FILE_TYPE] = file_type
    doc[FILENAME] = file_name
    doc[SOURCE] = source
    for key, value in additional_data.items():
        if key not in document_source_field_keys:
            doc[key] = value
    doc.save()


def save_extracted_content(
    source_url: str, content: str, extraction_method: ExtractionMetod
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
