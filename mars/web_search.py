import time
import urllib

import undetected_chromedriver as uc
from googlesearch import search


def get_google_first_result_bs(query: str) -> str:
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
