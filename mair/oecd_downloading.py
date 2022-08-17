"""Functions for downloading data from OECD AI Policy Observatory."""
import json
import ssl
from typing import Dict, List

import requests

from mair.db import db_fields

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
URL = "https://api.oecd.ai/ws/AIPO/API/dashboards/policyInitiatives.xqy?conceptUris=undefined"


def parse_result_dict(result: dict) -> Dict[str, str]:
    """Parse metadata from OECD."""
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


def download() -> List[Dict[str, str]]:
    """Returns list of dicts with oecd api results."""

    ssl._create_default_https_context = ssl._create_unverified_context
    res = requests.get(URL, verify=False)

    data = json.loads(res.text)

    parsing_results = [parse_result_dict(result) for result in data["results"]]
    parsing_results = [p for p in parsing_results if p[db_fields.URL] is not None]

    return parsing_results
