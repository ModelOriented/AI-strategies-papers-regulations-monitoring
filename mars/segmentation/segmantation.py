"""Split all pdfs from database"""
import typer
import time
import mars
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


def segment_and_upload(key_min, key_max) -> None:
    get_done_query = f"FOR u IN {collections.SEGMENTED_TEXTS} RETURN u.{DOC_ID}"
    done_docs = mars.db.database.AQLQuery(get_done_query, 10000, rawResults=True)
    done_docs = set(list(done_docs))

    all_docs_query = f"FOR u IN {collections.DOCUMENTS} FILTER TO_NUMBER(u._key) >= {key_min} && TO_NUMBER(u._key) <= {key_max} RETURN u"
    all_docs = mars.db.database.AQLQuery(all_docs_query, 10000, rawResults=True)
    all_docs = set(list(all_docs))

    todo_docs = [doc for doc in all_docs if doc[ID] not in done_docs]

    logger.info(
        "Already segmented documents: %s / %s"
        % (len(all_docs) - len(todo_docs), len(all_docs))
    )
    logger.info(
        "Waiting for segmentation documents: %s / %s" % (len(todo_docs), len(all_docs))
    )
    for index, doc in enumerate(todo_docs):
        try:
            logger.info(
                "Segmenting %s (%s%%)"
                % (doc[ID], round(100 * index / len(todo_docs), 1))
            )

            start_time = time.time()
            with FileSync(doc[FILENAME]) as filename:
                if doc[FILE_TYPE] == db_fields.FileType.html:
                    segs = segment_html(filename)
                elif doc[FILE_TYPE] == db_fields.FileType.pdf:
                    segs = segment_pdf(filename, ROUND_DIGIT)
            parsed_time = time.time()
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
            end_time = time.time()
            logger.info(
                "Parse time: %ss | Save time: %ss"
                % (int(parsed_time - start_time), int(end_time - parsed_time))
            )
        except Exception as e:
            logger.error("Segmentation of document %s has failed:" % doc[ID])
            logging.log_exception("", e, logger)
