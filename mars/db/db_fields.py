from enum import Enum

# All:
ID = "_id"  # id is unique accross the whole database
KEY = "_key"  # key is unique accross the collection


def id_to_key(id: str) -> str:
    """Function convert database-unique document id to collection-unique document key"""
    return id.split("/")[-1]


# Source document:
URL = "url"
FILENAME = "filename"
FILE_TYPE = "file_type"
SOURCE = "source_website"
CONTENT = "content"
EXTRACTION_METHOD = "extraction_method"
COUNTRY = "country"
TITLE = "title"
USER = "user"

# Processed texts:
TEXT_ID = "textId"
SENTENCES = "sentences"
EMBEDDINGS = "embeddings"
DOC_ID = "source_doc_id"


# Annotation:
PROCESSED_TEXT_ID = "processedTextId"
SENTENCE = "sentence"
SENTENCE_SAMPLING_SCORE = "score"
SENT_NUM = "sentNum"
QUERY_TARGET = "queryTarget"
ANNOTATION_RESULT = "annotation_result"


class EmbeddingType(str, Enum):
    LASER = "laser"
    LABSE = "labse"


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
