import unittest
from mars.utils import *
import undetected_chromedriver as uc


class ScraperMethods(unittest.TestCase):
    def test_search_for_url(self):
        url = search_for_url(
            "Everybody loves good https://en.wikipedia.org/wiki/Jellyfish because why not"
        )
        self.assertEqual("https://en.wikipedia.org/wiki/Jellyfish", url)

        url = search_for_url("No url here!")
        self.assertEqual(None, url)

    def test_duck_duck_go_search(self):
        "First run of this may be long"
        driver = uc.Chrome()
        result = get_duckduckgo_first_result(driver, "jellyfish")
        self.assertIsInstance(result, str)

    def test_fetching_semantic_scholar(self):
        paper = fetch_paper_information("1408.2083")
        self.assertEqual(paper[0]["arxivId"], "1408.2083")


if __name__ == "__main__":
    unittest.main()
