from mars.db import db_fields
import newspaper
from bs4 import BeautifulSoup
import re


def segment_html(
    filename, extraction_method=db_fields.ExtractionMetod.newspaper
) -> list:

    article = newspaper.Article(url=" ", language="en", keep_article_html=True)
    with open(filename, mode="r") as f:
        raw_html = f.read()

    article.set_html(raw_html)
    article.parse()
    processed_html = article.article_html
    soup = BeautifulSoup(processed_html)
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
