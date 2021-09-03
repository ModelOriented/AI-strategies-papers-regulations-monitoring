"""Split all pdfs from database"""
from typer.models import FileTextWrite
from mars import logging, config
import typer
from mars.pdf_segmentation import segment_pdf
from mars.db import collections, database
from mars.db.db_fields import (
    CONTENT,
    FILENAME,
    FILE_TYPE,
    HTML_TAG,
    ID,
    IS_HEADER,
    TEXT_ID,
    FileType,
)

logger = logging.getLogger(__name__)
level = logging.getLevelName(config.logging_level)
logger.setLevel(level)
logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S")


ROUND_DIGIT = 1


def segment_and_upload():
    for doc in collections.document_sources.fetchByExample(
        {FILE_TYPE: FileType.pdf}, batchSize=100
    ):
        try:
            if (
                len(collections.segmented_texts.fetchFirstExample({TEXT_ID: doc[ID]}))
                == 1
            ):
                print("Skipping %s" % doc[ID])
                # text already segmented - ommiting
                continue
            segs = segment_pdf(doc[FILENAME], ROUND_DIGIT)

            for s in segs:
                segmented_doc = collections.segmented_texts.createDocument()
                segmented_doc[TEXT_ID] = doc[ID]
                segmented_doc[HTML_TAG] = s["html_tag"]
                segmented_doc[CONTENT] = s["content"]
                segmented_doc[FILE_TYPE] = FileType.pdf
                if "h" in s["html_tag"]:
                    segmented_doc[IS_HEADER] = True
                else:
                    segmented_doc[IS_HEADER] = False
                segmented_doc.save()
        except Exception as e:
            logging.log_exception("", e, logger)


if __name__ == "__main__":
    typer.run(segment_and_upload)
