import json
import os
import re
import ssl
import time
import urllib

import pdfminer.converter
import pdfminer.layout
import pdfminer.pdfinterp
import pdfminer.pdfpage
import requests
import semanticscholar as sch
import undetected_chromedriver as uc
from googlesearch import search

from mars.db import db_fields

URL = "https://www.oecd.ai/ws/AIPO/API/dashboards/policyInitiatives.xqy?conceptUris=undefined"


def parse_result_dict(result):
    """ """
    name = result["label"]
    oecd_id = result["uri"].split("/")[-1]
    # description = result["description"]
    for field in result["fields"]:
        key = field["key"]
        value = field["value"]
        if key == "Country":
            country = value
        elif key == "Public access URL":
            document_url = value
        elif key == "Cover start date":
            start_date = value
        elif key == "Cover end date":
            end_date = value

    document_info = {
        db_fields.TITLE: name,
        db_fields.COUNTRY: country,
        db_fields.URL: document_url,
        "startDate": start_date,
        "endDate": end_date,
        "oecdId": oecd_id,
    }
    return document_info


def get_oecd_parsing_results():
    # TODO: move to oecd-scraping, or scraper.py
    """
    Returns list of dicts with oecd api results
    """

    ssl._create_default_https_context = ssl._create_unverified_context
    res = requests.get(URL, verify=False)

    data = json.loads(res.text)

    parsing_results = [parse_result_dict(result) for result in data["results"]]
    parsing_results = [p for p in parsing_results if p[db_fields.URL] is not None]

    return parsing_results


def get_number_of_files(dir: str):
    """
    Returns number of files in dir
    Excludes .part files
    """
    if os.path.exists(dir):
        files = [f for f in os.listdir(dir) if ".part" not in f]
        n_files = len(files)
    else:
        n_files = 0
    return n_files


def split_on_words(text, split_words):
    """Splits text on certain words, essentially treats them like buckets"""

    threads = dict.fromkeys(split_words)

    for i in range(len(list(threads.keys())) - 1):
        earlier_thread = list(threads.keys())[i]
        split_thread = list(threads.keys())[i + 1]
        splitted = text.split(split_thread, 1)
        threads[earlier_thread] = splitted[0]

        text = splitted[1]

    return threads


# extract text from PDF


def extract_text_from_pdf(file_name: str) -> dict:
    """Extract text and other attributes from pdf in form od dict"""
    # TODO: Move to parser.py
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


def search_for_url(string: str):
    """
    Searches for url in a string. If a string has query inside than returns aforementioned url. If not than
    returns None
    """
    result = re.search(
        "(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?",
        string,
    )
    if result is not None:
        return result.group()
    else:
        return None


def get_google_first_result_bs(query):
    """
    Omits results with from google scholar, unfortunately id does not omit adds.
    Uses library based on beautifulsoup. After some searches it might be detected.
    """
    query = urllib.parse.quote_plus(query)
    result = [None, None]
    for i, val in enumerate(search(query, num=2, stop=2)):
        result[i] = val
    if result[0][:22] != "https://scholar.google":
        return result[0]
    else:
        return result[1]


def get_google_first_result_selenium(query):
    """
    Uses undetected chrome browser to query google and fetch the first link. Omits google
    scholar. Does not omit adds.
    Only limited number of searches is available, until it gives capcha.
    """
    query = urllib.parse.quote_plus(query)

    def search_for_item(query, which):
        driver = uc.Chrome()
        driver.get("https://www.google.com/search?q=" + query + "&start=" + str(0))
        time.sleep(2)
        driver.find_element_by_xpath('//*[@id="L2AGLb"]').click()
        item = driver.find_elements_by_css_selector("h3")[which]
        item.click()
        time.sleep(2)
        result = driver.current_url

        driver.quit()
        return result

    result = search_for_item(query, 0)

    if result[:22] == "https://scholar.google":
        result = search_for_item(query, 1)

    return result


def get_duckduckgo_first_result(driver, query: str) -> str:
    """
    Gets url from the first (top) result of duckduckgo.

    @param driver: selenium driver, advised to use undetected-chromedriver
    @param query: some query to be searched, it might be plain text.
    @return: single url
    """
    query = urllib.parse.quote_plus(query)
    driver.get(f"https://duckduckgo.com/?q={query}&t=h_&ia=web")
    result = driver.find_element_by_class_name("result__a").get_attribute("href")
    return result


def get_inteligent_first_search_results(queries: list) -> dict:
    """
    Intelligently searches for a list of queries using
    BeautifulSoup (bs) + google, Selenium + google and Selenium + DuckDuckGo.
    If one component fails switches to the next one. So if both Selenium and BeautifulSoup
    fail with google, Selenium will continue with DuckDuckGo

    @param queries: list of queries
    @return: dict with matches and the search method
    """
    basic_google_down = False
    selenium_google_down = False

    duck_duck_driver = False

    results = {}
    # first basic search:
    for query in queries:

        if not basic_google_down:
            try:
                time.sleep(2)
                result = (get_google_first_result_bs(query), "beautifulsoupgoogle")

            except:
                basic_google_down = True
                print("BS google is down")

        if not selenium_google_down and basic_google_down:
            try:
                result = (get_google_first_result_selenium(query), "seleniumgoogle")
            except:
                selenium_google_down = True

                print("Selenium Google is down")

        if selenium_google_down and basic_google_down:
            if duck_duck_driver is False:
                duck_duck_driver = uc.Chrome()

            result = (
                get_duckduckgo_first_result(duck_duck_driver, query),
                "duckduckgo",
            )

        results[query] = result

    return results


def fetch_paper_information(arxiv_id: str):
    """Fetches paper information based on semantic scholar api"""
    print("Fetching: {}".format(arxiv_id))
    time.sleep(2.5)
    paper = sch.paper(
        "arXiv:{}".format(arxiv_id), timeout=10, include_unknown_references=True
    )
    try:
        return (
            paper,
            [p["arxivId"] for p in paper["references"] if p["arxivId"] is not None],
        )
    except KeyError:
        return paper, []
