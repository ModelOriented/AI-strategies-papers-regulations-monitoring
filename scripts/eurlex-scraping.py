from mars.scraper import Scraper
from mars import db
import urllib
import requests
import dotenv
import logging
import os
import traceback

dotenv.load_dotenv()
logger = logging.getLogger(__name__)
level = logging.getLevelName(os.getenv("LOGGING_LEVEL"))
logger.setLevel(level)
logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S")


def get_eurlex_search_link(keyword: str, lang: str):
    url = (
        "https://eur-lex.europa.eu/search.html?scope=EURLEX&text=%22"
        + keyword.replace(" ", "-")
        + "%22&lang="
        + lang
        + "&type=quick"
    )
    return url


def scrap_eurlex_single_page(s: Scraper):
    celexes = s.driver.find_elements_by_xpath(
        "//*[contains(text(), 'CELEX')]/following-sibling::dd"
    )
    celexes = [x.text for x in celexes]

    for x in celexes:
        if 5 < len(x) < 16:
            try:
                url_ = URL_POINT + urllib.parse.quote(x)
                logger.info("Scraping %s" % url_)
                s.save_content(url_, db.SourceWebsite.eurlex)

                # extract content
                # @TODO extracting simple content

            except Exception as e:
                print("Failed, err: %s" % e)
                traceback.print_exc()
        else:
            logger.debug("Invalid CELEX number")


def go_to_eurlex_page(s: Scraper, i: int, based: str):
    s.driver.get(based + "&page=" + str(i))


# @TODO
keywords = ["Artificial Intelligence", "AI"]
langs = ["en"]
URL_POINT = "http://publications.europa.eu/resource/celex/"

for lang in langs:
    for keyword in keywords:
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

        scrap_eurlex_single_page(s)

        for i in range(2, int(last_page), 1):
            go_to_eurlex_page(s, i, based)
            scrap_eurlex_single_page(s)
