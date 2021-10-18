"""Split all pdfs from database"""
import typer
from mars import config, logging
from mars.db import collections, db_fields
from mars.db.db_fields import (
    CONTENT,
    DOC_ID,
    FILE_TYPE,
    FILENAME,
    HTML_TAG,
    ID,
    IS_HEADER,
    SEQUENCE_NUMBER,
)
from mars.segmentation.html_segmantation import segment_html
from mars.segmentation.pdf_segmentation import segment_pdf
from mars.storage import FileSync

logger = logging.new_logger(__name__)

ROUND_DIGIT = 1


def segment_and_upload() -> None:
    for doc in collections.document_sources.fetchAll():
        try:
            if (
                len(collections.segmented_texts.fetchFirstExample({DOC_ID: doc[ID]}))
                == 1
            ):
                print("Skipping %s" % doc[ID])
                # text already segmented - ommiting
                continue
            with FileSync(doc[FILENAME]) as filename:
                if doc[FILE_TYPE] == db_fields.FileType.html:
                    segs = segment_html(filename)
                elif doc[FILE_TYPE] == db_fields.FileType.pdf:
                    segs = segment_pdf(filename, ROUND_DIGIT)

            for s in segs:
                segmented_doc = collections.segmented_texts.createDocument()
                segmented_doc[DOC_ID] = doc[ID]
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
            logger.error("Segmentation of document %s has failed:" % doc[ID])


if __name__ == "__main__":
    typer.run(segment_and_upload)
