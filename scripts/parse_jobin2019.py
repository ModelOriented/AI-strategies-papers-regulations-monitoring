import os
import re
from typing import List

import numpy as np
import pandas as pd
import typer
from dotenv import load_dotenv

from mars.db import db_fields
from mars.parser import extract_text_from_pdf
from mars.scraper import Scraper
from mars.utils import fetch_paper_information, split_on_words
from mars.utils import search_for_url
from mars.web_search import get_inteligent_first_search_results


def get_longest(text_list: list) -> List[int]:
    lengths = []
    for text in text_list:
        lengths.append(len(text))
    lengths = np.array(lengths)
    return text_list[int(np.argmax(lengths))]


def extract_references_from_jobin2019(file_name: str) -> List[str]:
    def split(txt, seps):
        default_sep = seps[0]

        # we skip seps[0] because that's the default separator
        for sep in seps[1:]:
            txt = txt.replace(sep, default_sep)
        return [i.strip() for i in txt.split(default_sep)]

    text = extract_text_from_pdf(file_name)["all_text"]
    text = text.split("REFERENCES")[1]
    text = text.split("Supplementary Information")[0]
    separators = re.findall("\\n\d+\.", text)
    references = split(text, separators)

    return references


def match_references_substring(ref1: list, ref2: list) -> dict:
    """
    From two lists of references makes a dict with pairs of matching indices.
    The matching is done in
    """
    refs = {}
    if ref1[0] == "":
        start = 1
    else:
        start = 0
    for i in range(start, len(ref1)):
        r1 = ref1[i]
        refs[i] = None
        for j, r2 in enumerate(ref2):
            if r2.lower() in r1.lower():
                refs[i] = j

    return refs


def extract_citations_from_jobin2019(file_name: str) -> dict:
    """Extracts citation numbers from jobin2019 - preprint version"""

    # get text
    separated_text = extract_text_from_pdf(file_name)["separated_text"]

    # getting the longest - the proper text on page
    text_on_pages = [get_longest(st) for st in separated_text]
    text_on_pages = np.array(text_on_pages)

    # split words like buckets
    # everything above justice fairness and equity is Transparency
    # later it is excluded from text and everything above Non-maleficience is Justice, fairness, and equity, etc.

    split_words = list(get_issue_mapping().values())
    split_words.append("Discussion")

    # clean
    for i, text in enumerate(text_on_pages):
        text = text.replace("\n", "")
        text = text.replace("(cf. Table 2)", "")
        text = text.replace("1.5", "")

        text_on_pages[i] = text

    relevant_text = " ".join(text_on_pages[7:13])
    threads = split_on_words(relevant_text, split_words)

    # pop empty one
    threads.pop("Discussion")

    all_citations = dict.fromkeys(list(threads.keys()))
    for key, text in threads.items():

        citations = np.array(re.findall(r"\d+", text))
        additional_citations = re.findall(r"\d+[–]\d+", text)
        for ac in additional_citations:
            range_ = re.findall(r"\d+", ac)
            lower, higher = int(range_[0]), int(range_[1])
            between = np.arange(lower + 1, higher)
            citations = np.append(citations, between)
        all_citations[key] = citations

    for key, ac in all_citations.items():
        ac = np.unique(ac)
        all_citations[key] = ac

    return all_citations


def match_references_with_url(references: list, arxiv_id: str) -> dict:
    """
    Matches references from jobin2019 and version from semantic scholar to obtain urls.
    If no url is found it searches google and duckduckgo using inteligent search.
    @param references: list of references from article in form of strings
    @param arxiv_id: string, arxiv id of jobin2018
    @return: returns dict with matches and information about where the match was found
    """

    for i, ref in enumerate(references):
        references[i] = ref.replace("\n", "")

    paper_info = fetch_paper_information(arxiv_id)
    references_scholar = paper_info[0]["references"]

    references_scholar_titles = [rs["title"] for rs in references_scholar]

    refs = match_references_substring(references, references_scholar_titles)
    links = {}

    for i, j in refs.items():

        try:
            if refs[i] is not None:
                links[i] = references_scholar[j]["url"]
            else:
                links[i] = search_for_url(references[i])

            if links[i] == "" or links[i] is None:
                links[i] = search_for_url(references[i])
                if links[i] is None:
                    raise Exception
        except:
            links[i] = None

    unresolved_links = {}
    for k, v in links.items():
        if v is None:
            unresolved_links[k] = references[k]

    results = get_inteligent_first_search_results(list(unresolved_links.values()))

    i = 0
    for link_id, value in unresolved_links.items():
        links[link_id] = list(results.values())[i]
        i += 1

    return links


def get_issue_mapping():
    mapping = {
        0: "Transparency",
        1: "Justice, fairness, and equity",
        2: "Non-maleficence",
        3: "Responsibility and accountability",
        4: "Privacy",
        5: "Beneficence",
        6: "Freedom and autonomy",
        7: "Trust",
        8: "Sustainability",
        9: "Dignity",
        10: "Solidarity",
    }
    return mapping


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

    mapping = get_issue_mapping()

    for key1, val1 in citations_dict.items():
        for key2, val2 in mapping.items():
            if np.isin(key1, citations[val2]):
                citations_dict[key1][key2] = 1

    data = pd.DataFrame(citations_dict).T.astype(int)
    data.columns = mapping.values()

    return data


def parse_and_save():
    """
    Parses and saves data from jobin2019 extraction to csv in data folder.
    Saves a csv file in mars/data
    """
    load_dotenv(".env")
    file_name = os.environ["JOBIN2019_LOCATION"]
    data = extract_topics_from_jobin_citations(file_name)
    references = extract_references_from_jobin2019(file_name)
    actual_references = list(np.array(references)[data.index])
    # begin searching:
    actual_link = match_references_with_url(actual_references, "1906.11668")
    for key, ac in actual_link.items():
        if type(ac) == str:
            actual_link[key] = (ac, "semantic_scholar_api")
    data["title"] = [ar for ar in actual_references]
    data["link"] = [ac[0] for ac in actual_link.values()]
    data["source"] = [ac[1] for ac in actual_link.values()]
    data.to_csv("data/jobin2019.csv")


def upload_to_database():
    jobin = pd.read_csv('data/jobin2019.csv')
    links = jobin['link']
    scraper = Scraper()
    for link in links:
        scraper.save_document(url=link, source=db_fields.SourceWebsite.manual, metadata={'dataset':'jobin2019'})


if __name__ == "__main__":
    typer.run(parse_and_save)
    typer.run(upload_to_database)
