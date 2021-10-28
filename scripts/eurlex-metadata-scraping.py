import requests
from lxml import etree
from mars.db import collections
import mars.db.db_fields as db_fields
from mars import logging

url = "http://publications.europa.eu/resource/celex/%s?language=eng"

for doc in collections.document_sources.fetchByExample(
    {db_fields.SOURCE: db_fields.SourceWebsite.eurlex}, batchSize=1000
):
    try:
        if not doc[db_fields.TITLE]:
            url_celex = url % doc["celex"]
            r = requests.get(url, headers={"Accept": "application/xml;notice=object"})
            r.raise_for_status()
            root = etree.fromstring(r.content)

            doc[db_fields.COUNTRY] = root.find(".//CREATED_BY/PREFLABEL").text
            doc[db_fields.TITLE] = root.find(".//EXPRESSION_TITLE/VALUE").text
            doc[db_fields.START_DATE] = root.find(".//WORK_DATE_DOCUMENT/VALUE").text

    except Exception as e:
        logging.log_exception("", e)
