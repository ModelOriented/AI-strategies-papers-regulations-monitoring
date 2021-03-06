"""Extracting content from HTMLs and PDFs"""

import glob
import os
from abc import ABC
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import List

import newspaper
import pdfminer
from pdfminer import converter, layout, pdfinterp, pdfpage

import mars.db as db
import mars.logging
from mars.storage import FileSync

logger = mars.logging.new_logger(__name__)


class HTMLFilter(HTMLParser, ABC):
    text = ""

    def handle_data(self, data):
        """
        @param data: string
        """
        self.text += data


def parse_html(source_url: str, method: db.ExtractionMethod) -> None:
    """
    Parses html file using extraction method, saves result to db.
    @param source_url: string
    @param method: db.ExtractionMethod
    @return: None
    """

    if db.is_content_present(source_url, method):
        logger.info("Skipping %s - already parsed via %s" % (source_url, method))
        return

    # get file from database
    doc = db.collections.document_sources.fetchFirstExample({db.URL: source_url})[0]

    file_id = doc[db.FILENAME]
    # read file
    with FileSync(file_id) as filename:
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
    """Extracts text and metadata from *.pdf file, saves results to dv.
    @param source_url: str
    @param method: db.ExtractionMethod
    @return: None
    """

    if db.is_content_present(source_url, method):
        return

    doc = db.collections.document_sources.fetchFirstExample({db.URL: source_url})[0]
    file_id = doc[db.FILENAME]
    with FileSync(file_id) as file_name:
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
    """
    Add files to database from path
    @param path: string
    """
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


def parse_documents(filter: dict, batch_size: int = 100):
    """
    Parse documents in database
    @param filter: dict to specify documents
    @param batch_size: int
    """
    for doc in db.collections.document_sources.fetchByExample(
        filter, batchSize=batch_size
    ):
        logger.info("Parsing %s" % doc[db.URL])
        if doc[db.FILE_TYPE] == db.FileType.pdf:
            parse_pdf(doc[db.URL], db.ExtractionMethod.pdfminer)

        elif doc[db.FILE_TYPE] == db.FileType.html:
            parse_html(doc[db.URL], db.ExtractionMethod.newspaper)


def parse_source(source: str, batch_size: int):
    """
    Parse document from database from source
    @param source: str
    @param batch_size: int
    """
    parse_documents({db.SOURCE: source}, batch_size=batch_size)


def extract_text_from_pdf(file_name: str) -> dict:
    """Extract text and other attributes from pdf in form od dict
    @param file_name:  string
    @return: dict
    """
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

    document_dict = {
        "all_text": all_text,
        "text_on_page": text_on_page,
        "empty_pages": empty_pages,
        "page_no": page_no,
        "separated_text": separated_text,
    }
    return document_dict
