import unittest
from mars.parser import extract_text_from_pdf

class Parser(unittest.TestCase):

    def test_extract_text_from_pdf(self):
        result = extract_text_from_pdf('tests/test_document.pdf')
        self.assertEqual(result['all_text'].strip(), 'test')

if __name__ == "__main__":
    unittest.main()