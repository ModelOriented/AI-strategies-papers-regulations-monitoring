import os
from pathlib import Path
import re
import time
import semanticscholar as sch

ROOT_DIR = Path(__file__).absolute().parent.parent


def set_root_path():
    """Sets the path of the runned code to main project path. Useful especially in notebooks."""
    os.chdir(ROOT_DIR)


def get_number_of_files(dir: str) -> int:
    """
    Returns number of files in dir
    Excludes .part files
    """
    if os.path.exists(dir):
        files = [f for f in os.listdir(dir) if ".part" not in f]
        n_files = len(files)
    else:
        n_files = 0
    return n_files


def split_on_words(text: str, split_words: list) -> dict:
    """Splits text on certain words, essentially treats them like buckets"""

    threads = dict.fromkeys(split_words)
    for i in range(len(list(threads.keys())) - 1):
        earlier_thread = list(threads.keys())[i]
        split_thread = list(threads.keys())[i + 1]
        splitted = text.split(split_thread, 1)
        threads[earlier_thread] = splitted[0]

        text = splitted[1]

    return threads


# extract text from PDF


def search_for_url(string: str):
    """
    Searches for url in a string. If a string has query inside than returns aforementioned url. If not than
    returns None
    """
    result = re.search(
        "(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?",
        string,
    )
    if result is not None:
        return result.group()
    else:
        return None


def fetch_paper_information(arxiv_id: str):
    """Fetches paper information based on semantic scholar api"""
    print("Fetching: {}".format(arxiv_id))
    time.sleep(2.5)
    paper = sch.paper(
        "arXiv:{}".format(arxiv_id), timeout=10, include_unknown_references=True
    )
    try:
        return (
            paper,
            [p["arxivId"] for p in paper["references"] if p["arxivId"] is not None],
        )
    except KeyError:
        return paper, []
