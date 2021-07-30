import glob
import os
from dataclasses import dataclass
from typing import List

import pdfminer.converter
import pdfminer.layout
import pdfminer.pdfinterp
import pdfminer.pdfpage

import mars.db as db
import dragnet
import newspaper
import logging

logger = logging.getLogger(__name__)

logger.setLevel(logging.getLevelName(os.getenv("LOGGING_LEVEL")))
logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %H:%M:%S")


def parse_html(source_url: str, method: db.ExtractionMetod) -> None:
    """
    Parses html file using extraction method
    """

    if db.is_content_present(source_url):
        return

    # get file from database
    filename = db.documentSources.fetchFirstExample({db.URL: source_url})[0]

    # read file
    with open(filename, "r") as f:
        raw_html = f.read()

    if method == db.ExtractionMetod.dragnet:
        content = dragnet.extract_content(raw_html)
    elif method == db.ExtractionMetod.newspaper:
        article = newspaper.Article(url=" ", language="en", keep_article_html=True)
        article.set_html(raw_html)
        article.parse()
        content = article.text

    db.save_extracted_content(source_url, content=content, extraction_method=method)


# PDF parsing imported from MAIR project


def parse_pdf(source_url: str, method: db.ExtractionMetod) -> None:
    """Extracts text and metadata from *.pdf file"""

    if db.is_content_present(source_url):
        return

    doc = db.documentSources.fetchFirstExample({db.URL: source_url})[0]
    file_name = doc[db.FILENAME]

    empty_pages = []
    separated_text = []
    all_text = ""
    page_no = 0
    document = open(file_name, "rb")
    rsrcmgr = pdfminer.pdfinterp.PDFResourceManager()
    laparams = pdfminer.layout.LAParams()
    device = pdfminer.converter.PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = pdfminer.pdfinterp.PDFPageInterpreter(rsrcmgr, device)
    for page in pdfminer.pdfpage.PDFPage.get_pages(document):
        text_on_page = []
        interpreter.process_page(page)
        layout = device.get_result()
        for element in layout:
            if isinstance(element, pdfminer.layout.LTTextBoxHorizontal):
                text_on_page.append(element.get_text())
                all_text += element.get_text()
        if len(text_on_page) == 0:
            empty_pages.append(page_no)
        separated_text.append(text_on_page)
        page_no += 1
    document.close()

    # leave for future meta
    # return Pdf(separated_text, empty_pages, all_text)
    db.save_extracted_content(source_url, content=all_text, extraction_method=method)


@dataclass
class Pdf:
    pages: List[List[str]]
    empty_pages: list
    full_text: str


def add_missing_files_to_db(path: str):
    for filename in glob.glob(os.path.join(path, "*.pdf")):
        try:
            if not db.is_document_present(filename):
                # add raw file to db
                db.save_doc(
                    filename,
                    filename,
                    db.FileType.pdf,
                    source=db.SourceWebsite.localhost,
                )

                # pass filename as source
                parse_pdf(filename, db.ExtractionMetod.pdfminer)
        except Exception as e:
            print("Fail to parse %s, error: %s" % (filename, e))
            continue

    for filename in glob.glob(os.path.join(path, "*.html")):
        try:
            if not db.is_document_present(filename):
                # add raw file to db
                db.save_doc(
                    filename,
                    filename,
                    db.FileType.html,
                    source=db.SourceWebsite.localhost,
                )

                # pass filename as source
                parse_html(filename, db.ExtractionMetod.dragnet)
                parse_html(filename, db.ExtractionMetod.newspaper)
        except Exception as e:
            print("Fail to parse %s, error: %s" % (filename, e))
            continue


def parse_source(source: str, batch_size: int):
    for doc in db.documentSources.fetchByExample(
        {db.SOURCE: source}, batchSize=batch_size
    ):
        logger.info("Parsing %s" % doc[db.URL])
        if doc[db.FILE_TYPE] == "FileType.pdf":
            parse_pdf(doc[db.URL], db.ExtractionMetod.pdfminer)

        elif doc[db.FILE_TYPE] == "FileType.html":
            parse_html(doc[db.URL], db.ExtractionMetod.dragnet)
            parse_html(doc[db.URL], db.ExtractionMetod.newspaper)

        else:
            continue
