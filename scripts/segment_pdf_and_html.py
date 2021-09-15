"""Split all pdfs from database"""
from typer.models import FileTextWrite
from mars import logging, config
import typer
from mars.db import db_fields
from mars.segmentation.pdf_segmentation import segment_pdf
from mars.segmentation.html_segmantation import segment_html
from mars.db import collections
from mars.db.db_fields import (
    CONTENT,
    FILENAME,
    FILE_TYPE,
    HTML_TAG,
    ID,
    IS_HEADER,
    SEQUENCE_NUMBER,
    TEXT_ID,
    FileType,
)

logger = logging.getLogger(__name__)
level = logging.getLevelName(config.logging_level)
logger.setLevel(level)
logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S")


ROUND_DIGIT = 1


def segment_and_upload():
    for doc in collections.document_sources.fetchAll():
        try:
            if (
                len(collections.segmented_texts.fetchFirstExample({TEXT_ID: doc[ID]}))
                == 1
            ):
                print("Skipping %s" % doc[ID])
                # text already segmented - ommiting
                continue

            if doc[FILE_TYPE] == db_fields.FileType.html:
                segs = segment_html(doc[FILENAME])
            elif doc[FILE_TYPE] == db_fields.FileType.pdf:
                segs = segment_pdf(doc[FILENAME], ROUND_DIGIT)

            for s in segs:
                segmented_doc = collections.segmented_texts.createDocument()
                segmented_doc[TEXT_ID] = doc[ID]
                segmented_doc[HTML_TAG] = s["html_tag"]
                segmented_doc[CONTENT] = s["content"]
                segmented_doc[SEQUENCE_NUMBER] = s["sequence_number"]

                segmented_doc[FILE_TYPE] = doc[FILE_TYPE]

                if "h" in s["html_tag"]:
                    segmented_doc[IS_HEADER] = True
                else:
                    segmented_doc[IS_HEADER] = False
                segmented_doc.save()
        except Exception as e:
            logging.log_exception("", e, logger)


if __name__ == "__main__":
    typer.run(segment_and_upload)
