import os
import uuid
from enum import Enum

from pyArango.connection import Connection

URL = "url"
FILENAME = "filename"
FILE_TYPE = "file_type"
SOURCE = "source_website"

FILES_DIR = "raw_data"


class SourceWebsite(str, Enum):
    oecd = "oecd"


class FileType(str, Enum):
    pdf = "pdf"
    html = "html"


conn = Connection(username="root", password="rootpassword")

db = conn.createDatabase(name="mars")
documentSources = db.createCollection(name="Documents")


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


def _new_file(file_content, file_type: str):
    filename = str(uuid.uuid4()) + file_type
    with open(os.path.join(FILES_DIR, filename), "w") as file:
        file.write(file_content)
    return filename
