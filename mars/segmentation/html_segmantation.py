import re

import newspaper
from bs4 import BeautifulSoup
from mars.db import db_fields
from mars import logging


logger = logging.new_logger(__name__)


def segment_html(
    filename, extraction_method=db_fields.ExtractionMethod.newspaper
) -> list:
    """
    Splits html file into list of html headers and paragraphs (h1-h6 and p tags)
    @param filename: string
    @param extraction_method: only newspaper supported
    @return: list of dicts with segments
    """

    # create and read html
    article = newspaper.Article(url=" ", language="en", keep_article_html=False)
    with open(filename, mode="r") as f:
        raw_html = f.read()
    article.set_html(raw_html)
    article.parse()
    if article.top_node is None:
        logger.info("Best top node of document is None")
        return []
    parser = article.config.get_parser()
    top_node = parser.nodeToString(article.top_node)

    # create segments
    soup = BeautifulSoup(top_node)
    segs = []
    counter = 0
    for header in soup.find_all([re.compile("^h[1-6]$"), "p"]):
        segs.append(
            {
                "html_tag": header.name,
                "content": header.get_text(),
                "sequence_number": counter,
            }
        )
        counter += 1
    return segs
