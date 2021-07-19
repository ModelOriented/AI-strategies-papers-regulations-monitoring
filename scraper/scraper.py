import datetime
import os
from selenium import webdriver


class Scraper:
    """
    Wrapper
    """

    def __init__(self, download_dir=".", headless=True, verbose=True):
        """
        Initializes firefox driver
        @param download_dir - path to directory where downloaded files will be saved
        @param headless - if driver should start without graphical interface
        @verbose
        """
        self.verbose = verbose
        self.print_log("Setting up driver")
        self.log_dir = "./logs"
        if not os.path.isfile(self.log_dir):
            os.mkdir(self.log_dir)

        profile = webdriver.FirefoxProfile()
        profile.set_preference("browser.download.folderList", 2)
        profile.set_preference("browser.download.manager.useWindow", False)
        profile.set_preference("browser.download.dir", download_dir)

        # tutaj dodac pdf
        mimetypes = ["application/pdf", "application/x-pdf"]
        profile.set_preference(
            "browser.helperApps.neverAsk.saveToDisk", ",".join(mimetypes)
        )

        options = webdriver.firefox.options.Options()
        # comment to allow firefox window
        if headless:
            options.add_argument("--headless")

        self.driver = driver = webdriver.Firefox(
            executable_path="./scraper/geckodriver",
            firefox_profile=profile,
            options=options,
        )

    def save_article(self, url: str, filename: str):
        """
        save html source to filename
        """
        try:
            self.driver.get(url)
            raw_html = self.driver.page_source

            with open(filename, "w") as text_file:
                text_file.write(raw_html)

            if self.verbose:
                print("Scraping %s" % url)

        except:
            self.save_snapshot()
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
