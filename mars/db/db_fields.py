from enum import Enum

URL = "url"
FILENAME = "filename"
FILE_TYPE = "file_type"
SOURCE = "source_website"
DOC_ID = "source_doc_id"
CONTENT = "content"
EXTRACTION_METHOD = "extraction_method"
USER = "user"


COUNTRY = "country"
TITLE = "title"


class SourceWebsite(str, Enum):
    oecd = "oecd"
    manual = "manually_added"
    eurlex = "eurlex"


class FileType(str, Enum):
    pdf = "pdf"
    html = "html"


class ExtractionMetod(str, Enum):
    newspaper = "newspaper3k"
    dragnet = "dragnet"
    pdfminer = "pdfminer"
    simple_html = "simple_html"
