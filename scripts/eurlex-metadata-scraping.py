import requests
from lxml import etree
from mars.db import collections
import mars.db.db_fields as db_fields
from mars import logging
from mars import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(config.logging_level))

url = "http://publications.europa.eu/resource/celex/%s?language=%s"

for doc in list(
    collections.document_sources.fetchByExample(
        {db_fields.SOURCE: db_fields.SourceWebsite.eurlex, db_fields.TITLE: None},
        batchSize=1000,
    )
):
    try:
        url_celex = url % (doc["celex"], doc["lang"])
        r = requests.get(url_celex, headers={"Accept": "application/xml;notice=object"})
        r.raise_for_status()
        root = etree.fromstring(r.content)

        doc[db_fields.COUNTRY] = root.find(".//CREATED_BY/PREFLABEL").text
        doc[db_fields.TITLE] = root.find(".//EXPRESSION_TITLE/VALUE").text
        doc[db_fields.START_DATE] = root.find(".//WORK_DATE_DOCUMENT/VALUE").text
        doc.patch()

    except Exception as e:
        logging.log_exception(
            "Failed to fetch metadata for CELEX: %s" % doc["celex"], e, logger
        )
