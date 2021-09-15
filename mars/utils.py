import json
import os
import ssl

import pdfminer.converter
import pdfminer.layout
import pdfminer.pdfinterp
import pdfminer.pdfpage
import requests

# from mars import db_fields

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


# extract text from PDF

def extract_text_from_pdf(file_name: str) -> dict:
    """Extract text and other attributes from pdf in form od dict"""
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
        'all_text': all_text,
        'text_on_page': text_on_page,
        'empty_pages': empty_pages,
        'page_no': page_no,
        'separated_text': separated_text
    }
    return document_dict
