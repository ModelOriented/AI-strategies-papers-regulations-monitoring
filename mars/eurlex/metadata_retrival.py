from typing import List

import requests
from lxml import etree

FETCH_URL_TEMPLATE = "http://publications.europa.eu/resource/celex/{}"


def fetch_metadata(celex: str) -> etree.Element:
    celex = celex.replace("/", "%2F")
    url = FETCH_URL_TEMPLATE.format(celex)
    r = requests.get(
        url,
        headers={"Accept": "application/xml;notice=object"},
    )
    if r.status_code == 200:
        return etree.fromstring(r.content)
    else:
        raise Exception("Could not fetch metadata", r.status_code)


def get_all_links(root_metadata: etree.Element) -> List[etree.Element]:
    links = root_metadata.findall(".//WORK/*[@type='link']")
    return links
