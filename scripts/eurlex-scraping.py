"""Scrap documents from eurlex"""
import urllib

from mars import config, db, logging, parser
from mars.scraper import Scraper

logger = logging.new_logger(__name__)


def get_eurlex_search_link(keyword: str, lang: str):
    """

    @param keyword: used to search in eurlex
    @param lang: languge of document
    @return: string with url
    """
    url = (
        "https://eur-lex.europa.eu/search.html?scope=EURLEX&text=%22"
        + keyword.replace(" ", "-")
        + "%22&lang="
        + lang
        + "&type=quick"
    )
    return url


def scrap_eurlex_single_page(s: Scraper, metadata: dict):
    """

    @param s: instance of Scraper
    @param metadata: additional dict to insert to dock
    """

    # find all celex numbers on page
    celexes = s.driver.find_elements_by_xpath(
        "//*[contains(text(), 'CELEX')]/following-sibling::dd"
    )
    celexes = [x.text for x in celexes]

    for x in celexes:

        # check if CELEX number is correct
        if 5 < len(x) < 16:
            try:
                url_ = URL_POINT + urllib.parse.quote(x)
                logger.info("Scraping %s" % url_)

                metadata["celex"] = x
                s.save_document(url_, db.SourceWebsite.eurlex, metadata=metadata)

                # extract content
                parser.parse_html(url_, db.ExtractionMethod.simple_html)

            except Exception as e:
                logging.log_exception("Failed, err:", e, logger)
        else:
            logger.debug("Invalid CELEX number")


def go_to_eurlex_page(s: Scraper, i: int, based: str):
    """

    @param s: instance of Scraper
    @param i: destination page number
    @param based: search url
    """
    s.driver.get(based + "&page=" + str(i))


# constans
keywords = ["Artificial Intelligence", "AI"]
langs = ["en"]
URL_POINT = "http://publications.europa.eu/resource/celex/"

for lang in langs:
    for keyword in keywords:
        # create url
        s = Scraper()
        u = get_eurlex_search_link(keyword, lang)
        s.driver.get(u)
        based = s.driver.current_url

        # get number of pages
        last_page = (
            s.driver.find_element_by_xpath("//*[@title='Last Page']")
            .get_attribute("href")
            .split("page=")[-1]
        )

        metadata = {"lang": lang, "keyword": keyword}

        # scrap fist page from default link
        scrap_eurlex_single_page(s, metadata)

        for i in range(2, int(last_page), 1):
            # scrap rest of pages by adding &page=i
            go_to_eurlex_page(s, i, based)
            scrap_eurlex_single_page(s, metadata)
