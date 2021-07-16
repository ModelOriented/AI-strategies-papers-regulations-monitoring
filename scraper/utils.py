import pandas as pd
import newspaper, requests, ssl, json  # , dragnet
from bs4 import BeautifulSoup


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
        "title": name,
        #   "description": description,
        "country": country,
        "documentUrl": document_url,
        "startDate": start_date,
        "endDate": end_date,
        "oecdId": oecd_id,
    }
    return document_info


def get_oecd_df():
    """
    Returns json with oecd api results
    """

    ssl._create_default_https_context = ssl._create_unverified_context
    res = requests.get(URL, verify=False)

    data = json.loads(res.text)

    df = pd.DataFrame([parse_result_dict(result) for result in data["results"]])
    dff = df[~df["documentUrl"].isna()]

    return dff


def parse_article(filename: str):
    """
    Parses html file using newspaper3k
    """

    # read file
    with open(filename, "r") as f:
        raw_html = f.read()

    article = newspaper.Article(url=" ", language="en", keep_article_html=True)
    article.set_html(raw_html)
    article.parse()

    # get links from article

    links = []
    soup = BeautifulSoup(article.article_html)
    for link in soup.findAll("a"):
        links.append(link.get("href"))

    return {
        "html": article.article_html,
        "text": article.text,
        "authors": article.authors,
        "title": article.title,
        "links": list(set(links)),
    }


def parse_content(filename: str):
    """
    @TODO
    Parses html file using dragnet
    """

    # read file
    with open(filename, "r") as f:
        raw_html = f.read()

    return
