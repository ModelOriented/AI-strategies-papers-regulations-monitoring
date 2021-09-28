"""Extracting content from HTMLs and PDFs"""

import glob
import os
from abc import ABC
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import List

import newspaper

import mars.db as db
import mars.logging
from mars.utils import extract_text_from_pdf

logger = mars.logging.new_logger(__name__)


class HTMLFilter(HTMLParser, ABC):
    text = ""

    def handle_data(self, data):
        self.text += data


def parse_html(source_url: str, method: db.ExtractionMethod) -> None:
    """
    Parses html file using extraction method, saves result to db.
    """

    if db.is_content_present(source_url, method):
        logger.info("Skipping %s - already parsed via %s" % (source_url, method))
        return

    # get file from database
    doc = db.collections.document_sources.fetchFirstExample({db.URL: source_url})[0]

    filename = doc[db.FILENAME]
    # read file
    with open(filename, "r") as f:
        raw_html = f.read()

    if method == db.ExtractionMethod.newspaper:
        article = newspaper.Article(url=" ", language="en", keep_article_html=True)
        article.set_html(raw_html)
        article.parse()
        content = article.text

    elif method == db.ExtractionMethod.simple_html:
        f = HTMLFilter()
        f.feed(raw_html)
        content = f.text

    db.save_extracted_content(source_url, content=content, extraction_method=method)


def parse_pdf(source_url: str, method: db.ExtractionMethod) -> None:
    """Extracts text and metadata from *.pdf file, saves results to dv."""

    if db.is_content_present(source_url, method):
        return

    doc = db.collections.document_sources.fetchFirstExample({db.URL: source_url})[0]
    file_name = doc[db.FILENAME]

    document_dict = extract_text_from_pdf(file_name)

    # leave for future meta
    # return Pdf(separated_text, empty_pages, all_text)
    db.save_extracted_content(
        source_url, content=document_dict["all_text"], extraction_method=method
    )


@dataclass
class Pdf:
    pages: List[List[str]]
    empty_pages: list
    full_text: str


def add_missing_files_to_db(path: str):
    for filename in glob.glob(os.path.join(path, "*.pdf")):
        try:
            if not db.is_document_present(filename):
                with open(filename, mode="rb") as file:
                    fileContent = file.read()

                # add raw file to db
                db.save_doc(
                    filename,
                    fileContent,
                    db.FileType.pdf,
                    source=db.SourceWebsite.manual,
                )

                # pass filename as source
                parse_pdf(filename, db.ExtractionMethod.pdfminer)
        except Exception as e:
            mars.logging.log_exception(
                "Fail to parse %s, error: % (filename)", e, logger
            )
            continue

    for filename in glob.glob(os.path.join(path, "*.html")):
        try:
            if not db.is_document_present(filename):
                # add raw file to db
                db.save_doc(
                    filename, filename, db.FileType.html, source=db.SourceWebsite.manual
                )

                # pass filename as source
                parse_html(filename, db.ExtractionMethod.dragnet)
                parse_html(filename, db.ExtractionMethod.newspaper)
        except Exception as e:
            mars.logging.log_exception(
                "Fail to parse %s, error: % (filename)", e, logger
            )
            continue


def parse_source(source: str, batch_size: int):
    for doc in db.collections.document_sources.fetchByExample(
        {db.SOURCE: source}, batchSize=batch_size
    ):
        logger.info("Parsing %s" % doc[db.URL])
        if doc[db.FILE_TYPE] == db.FileType.pdf:
            parse_pdf(doc[db.URL], db.ExtractionMethod.pdfminer)

        elif doc[db.FILE_TYPE] == db.FileType.html:
            parse_html(doc[db.URL], db.ExtractionMethod.newspaper)

        else:
            continue
