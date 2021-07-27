import datetime
import dragnet
import newspaper
from selenium import webdriver

from mars import db
import os
from dotenv import load_dotenv
import logging


load_dotenv()

# level = logging.getLevelName(os.getenv("LOGGING_LEVEL"))

# logger = logging.Logger(__name__)

# logger.setLevel(level)
# logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")


class Scraper:
    """
    Wrapper
    """

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
            format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
        )

        os.makedirs(self.log_dir, exist_ok=True)

        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.download.manager.useWindow", False)
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

    def save_article(self, url: str, source: db.SourceWebsite):
        """
        save html source to filename
        """
        try:
            present = db.is_document_present(url)
            if not present:
                if self.verbose:
                    self.logger.info("Scraping %s" % url)

                self.driver.get(url)
                raw_html = self.driver.page_source

                db.save_doc(url, raw_html, file_type=db.FileType.html, source=source)

            else:
                self.logger.info("Omitting %s - url already in database" % url)
        except:
            self.save_snapshot()
            raise

    """
    Parsing content
    """

    @staticmethod
    def parse_content(source_url: str, method: db.ExtractionMetod.newspaper):
        """
        Parses html file using newspaper3k
        """

        # get file from database
        filename = db.documentSources.fetchFirstExample({db.URL: source_url})[0]

        # read file
        with open(filename, "r") as f:
            raw_html = f.read()

        if method == db.ExtractionMetod.dragnet:
            content = dragnet.extract_content(raw_html)
        elif method == db.ExtractionMetod.newspaper:
            article = newspaper.Article(url=" ", language="en", keep_article_html=True)
            article.set_html(raw_html)
            article.parse()
            content = article.text

        db.save_extracted_content(source_url, content=content, extraction_method=method)

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
