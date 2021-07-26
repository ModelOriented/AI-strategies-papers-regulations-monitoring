#!/usr/bin/env python3
# coding: utf-8
import os

from mars.scraper import Scraper
from mars.utils import get_oecd_df
from mars import db
import logging
import dotenv

dotenv.load_dotenv()
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.getLogger(__name__).setLevel(os.getenv("LOGGING_LEVEL"))

if __name__ == "__main__":
    logging.info("Fetching oecd urls")
    df = get_oecd_df()
    api = Scraper(headless=True)

    for index, row in df.iterrows():
        if "pdf" in row["documentUrl"]:
            continue
        try:
            api.save_article(row["documentUrl"],  source=db.SourceWebsite.oecd)
        except:
            continue
