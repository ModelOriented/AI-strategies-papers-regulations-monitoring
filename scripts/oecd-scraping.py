#!/usr/bin/env python3

# coding: utf-8
import logging
import os

import dotenv
import typer

import mars.logging
import mars.parser as parser
from mars import config, db, db_fields
from mars.scraper import Scraper
from mars.utils import get_oecd_parsing_results

dotenv.load_dotenv()
logger = logging.getLogger(__name__)
level = logging.getLevelName(config.logging_level)
logger.setLevel(level)
logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S")


def main(headless: bool = True):
    logger.info("Fetching oecd urls")
    parsing_results = get_oecd_parsing_results()
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
    parser.parse_source(db.SourceWebsite.oecd, counter)


if __name__ == "__main__":
    typer.run(main)
