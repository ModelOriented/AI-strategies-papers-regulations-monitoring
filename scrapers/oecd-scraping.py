#!/usr/bin/env python3

# coding: utf-8
import mars.logging
import mars.parser
import typer
from mars import db, oecd_downloading
from mars.db import db_fields
from mars.scraper import Scraper

logger = mars.logging.new_logger(__name__)


def main(headless: bool = True):
    logger.info("Fetching oecd urls")
    parsing_results = oecd_downloading.download()
    api = Scraper(headless=True)
    counter = 0

    for result in parsing_results:
        try:
            api.save_document(
                result[db_fields.URL], source=db.SourceWebsite.oecd, metadata=result
            )
            counter += 1
        except Exception as e:
            mars.logging.log_exception("Exception ocured:", e, logger)

    logger.info("Scrapped all! Proceeding to parse contents...")
    mars.parser.parse_source(db.SourceWebsite.oecd, counter)


if __name__ == "__main__":
    typer.run(main)
