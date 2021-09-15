import unittest
from mars import scraper


class ScraperMethods(unittest.TestCase):
    def test_instance(self):
        s = scraper.Scraper()
        self.assertIsInstance(s, scraper.Scraper)
        s.driver.get(
            "https://stackoverflow.com/questions/46669455/pre-commit-hook-unit-test"
        )


if __name__ == "__main__":
    unittest.main()
