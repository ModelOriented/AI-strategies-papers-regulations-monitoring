import pdfminer
import numpy as np
import pdfminer.converter
import pdfminer.layout
import pdfminer.pdfinterp
import pdfminer.pdfpage
import re
import pandas as pd
from mars.utils import extract_text_from_pdf


def get_longest(text_list: list) -> list:
    lengths = []
    for text in text_list:
        lengths.append(len(text))
    lengths = np.array(lengths)
    return text_list[int(np.argmax(lengths))]


def extract_citations_from_jobin2019(file_name: str) -> dict:
    """Extracts citation numbers from jobin2019 - preprint version"""

    # get text
    separated_text = extract_text_from_pdf(file_name)['separated_text']

    # getting the longest - the proper text on page
    text_on_pages = [get_longest(st) for st in separated_text]
    text_on_pages = np.array(text_on_pages)

    # split words like buckets
    # everything above justice fairness and equity is Transparency
    # later it is excluded from text and everything above Non-maleficience is Justice, fairness, and equity, etc.
    split_words = [
        'Transparency',
        'Justice, fairness, and equity',
        'Non-maleficence',
        'Responsibility and accountability',
        'Privacy',
        'Beneficence',
        'Freedom and autonomy',
        'Trust',
        'Sustainability',
        'Dignity',
        'Solidarity',
        'Discussion']  # stopword - last one will be excluded

    threads = dict.fromkeys(split_words)

    # clean
    for i, text in enumerate(text_on_pages):
        text = text.replace('\n', '')
        text = text.replace('(cf. Table 2)', '')
        text = text.replace('1.5', '')

        text_on_pages[i] = text

    relevant_text = ' '.join(text_on_pages[7:13])

    for i in range(len(list(threads.keys())) - 1):
        earlier_thread = list(threads.keys())[i]
        split_thread = list(threads.keys())[i + 1]
        splitted = relevant_text.split(split_thread, 1)
        threads[earlier_thread] = splitted[0]

        relevant_text = splitted[1]

    # pop empty one
    threads.pop('Discussion')

    all_citations = dict.fromkeys(list(threads.keys()))
    for key, text in threads.items():

        citations = np.array(re.findall(r'\d+', text))
        additional_citations = re.findall(r'\d+[–]\d+', text)
        for ac in additional_citations:
            range_ = re.findall(r'\d+', ac)
            lower, higher = int(range_[0]), int(range_[1])
            between = np.arange(lower + 1, higher)
            citations = np.append(citations, between)
        all_citations[key] = (citations)

    for key, ac in all_citations.items():
        ac = np.unique(ac)
        all_citations[key] = ac

    return all_citations


def extract_topics_from_jobin_citations(path_to_jobin_file: str) -> pd.DataFrame:
    """Extracts topics for each relevant citation in jobin2019 preprint version"""
    citations = extract_citations_from_jobin2019(path_to_jobin_file)

    all_citations = np.array([])
    for c in citations.values():
        all_citations = np.append(all_citations, c)

    all_citations = np.sort(np.unique(all_citations).astype(int))

    citations_dict = dict.fromkeys(all_citations)

    for k in citations_dict.keys():
        citations_dict[k] = np.zeros(11)

    mapping = {
        0: 'Transparency',
        1: 'Justice, fairness, and equity',
        2: 'Non-maleficence',
        3: 'Responsibility and accountability',
        4: 'Privacy',
        5: 'Beneficence',
        6: 'Freedom and autonomy',
        7: 'Trust',
        8: 'Sustainability',
        9: 'Dignity',
        10: 'Solidarity'}

    for key1, val1 in citations_dict.items():
        for key2, val2 in mapping.items():
            if np.isin(key1, citations[val2]):
                citations_dict[key1][key2] = 1

    data = pd.DataFrame(citations_dict).T.astype(int)
    data.columns = mapping.values()

    return data