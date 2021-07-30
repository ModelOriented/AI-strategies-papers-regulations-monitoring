#!/usr/bin/env python3

# coding: utf-8
import os
from mars.scraper import Scraper
from mars.utils import get_oecd_df
import mars.parser as parser
from mars import db
import logging
import dotenv

dotenv.load_dotenv()
logger = logging.getLogger(__name__)
level = logging.getLevelName(os.getenv("LOGGING_LEVEL"))
logger.setLevel(level)
logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S")

if __name__ == "__main__":
    logger.info("Fetching oecd urls")
    df = get_oecd_df()
    api = Scraper(headless=True)

    for index, row in df.iterrows():
        try:
            api.save_content(row["documentUrl"], source=db.SourceWebsite.oecd)

        except:
            continue

    parser.parse_source("SourceWebsite.oecd", 1000)
