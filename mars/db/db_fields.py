from enum import Enum

# All:
ID = "_id"  # id is unique across the whole database
KEY = "_key"  # key is unique across the collection


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
DATASET = "dataset"
START_DATE = "startDate"
DOC_NAME = "name"
DOC_JOBS = "jobs"

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

SIMILARITIES = "similarities"
SIMILARITY_SCORE = "similarity_score"


# Segmented texts:
HTML_TAG = "html_tag"
IS_HEADER = "is_header"
SEQUENCE_NUMBER = "sequence_number"
LANGUAGE = "language"
SEGMENT_DOC_ID = "source_doc_id"

# Sentences:
SENTENCE_NUMBER = "sentence_number"
SEGMENT_ID = "source_segment_id"
SENTENCE_DOC_ID = "source_doc_id"
IS_DEFINITION = "is_definition"
EMBEDDING = "embedding"
ISSUES = "issues"


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


class ExtractionMethod(str, Enum):
    newspaper = "newspaper3k"
    pdfminer = "pdfminer"
    simple_html = "simple_html"


class IssueSearchMethod(str, Enum):
    LASER = EmbeddingType.LASER
    LABSE = EmbeddingType.LASER
    KEYWORDS = "keywords"
