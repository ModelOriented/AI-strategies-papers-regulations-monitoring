import datetime
import logging
import os
import urllib

import dragnet
import magic
import newspaper
import requests
from dotenv import load_dotenv
from selenium import webdriver

from mars import db

load_dotenv()


class Scraper:
    def __init__(self, headless=True, verbose=True):
        """
        Initializes firefox driver
        @param headless - if driver should start without graphical interface
        @param verbose
        """
        self.verbose = verbose
        self.print_log("Setting up driver")
        self.log_dir = os.getenv("SCRAPER_LOGS_DIR")
        self.logger = logging.getLogger(__name__)

        self.logger.setLevel(logging.getLevelName(os.getenv("LOGGING_LEVEL")))
        logging.basicConfig(
            format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %H:%M:%S"
        )

        os.makedirs(self.log_dir, exist_ok=True)
        os.makedirs(os.getenv("RAW_FILES_DIR"), exist_ok=True)

        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.download.dir", os.getenv("RAW_FILES_DIR"))
        mimetypes = ["application/pdf", "application/x-pdf"]
        profile.set_preference(
            "browser.helperApps.neverAsk.saveToDisk", ",".join(mimetypes)
        )

        options = webdriver.firefox.options.Options()
        # comment to allow firefox window
        if headless:
            options.add_argument("--headless")

        self.driver = webdriver.Firefox(
            executable_path=os.getenv("GECKODRIVER_PATH"),
            firefox_profile=profile,
            options=options,
        )

    def _save_html(self, url: str, source: db.SourceWebsite, meta=dict()):
        """
        save html source to filename
        """
        try:
            present = db.is_document_present(url)
            if not present:
                if self.verbose:
                    self.logger.info("Scraping %s" % url)

                self.driver.get(url)
                self.driver.implicitly_wait(15)
                raw_html = self.driver.page_source

                db.save_doc(
                    url,
                    raw_html,
                    file_type=db.FileType.html,
                    source=source,
                    additional_data=meta,
                )

            else:
                self.logger.info("Omitting - url already in database")
        except:
            self.save_snapshot()
            raise

    def _save_pdf(self, url: str, source: db.SourceWebsite, meta=dict()):
        """
        save pdf
        """
        try:
            present = db.is_document_present(url)
            if not present:
                self.logger.info("Scraping %s" % url)
                r = requests.get(url)
                content_type = r.headers["content-type"]

                if "application/pdf" in content_type:

                    file_tmp_name = os.path.join(
                        os.getenv("RAW_FILES_DIR"), "./tmp.pdf"
                    )
                    urllib.request.urlretrieve(url, file_tmp_name)

                    mime = magic.Magic(mime=True)
                    if mime.from_file(file_tmp_name) != "application/pdf":
                        raise TypeError("Not pdf")

                    with open(file_tmp_name, mode="rb") as file:
                        fileContent = file.read()

                    db.save_doc(
                        url, fileContent, db.FileType.pdf, source, additional_data=meta
                    )
                else:
                    raise TypeError("Not pdf")
            else:
                self.logger.info("Omitting - url already in database")
        except:
            self.save_snapshot()
            raise

    def save_document(self, url: str, source: db.SourceWebsite, metadata=dict()):
        """ """
        try:
            if "pdf" in url:
                try:
                    self._save_pdf(url, source=source, meta=metadata)
                    return
                except:
                    self.logger.info("Failed to save pdf, trying html")
                    pass
            self._save_html(url, source=source, meta=metadata)

        except:
            self.save_snapshot()
            self.logger.info("Failed scraping")
            raise

    """
    Logging
    """

    def print_log(self, *args, **kwargs):
        if self.verbose:
            print(*args, **kwargs, flush=True)

    def save_snapshot(self):
        name = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.%f")
        self.driver.save_screenshot(self.log_dir + "/" + name + ".png")
        with open(self.log_dir + "/" + name + ".html", "w") as html_file:
            html_file.write(self.driver.page_source)

    def close(self):
        try:
            self.driver.quit()
        except:
            self.print_log("Failed to quit driver")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is not None:
            self.save_snapshot()
        self.close()
