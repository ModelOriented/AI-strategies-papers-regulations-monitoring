import time
import semanticscholar as sch

def fetch_paper_information(arxiv_id : str):
    """Fetches paper information based on semantic scholar api"""
    print("Fetching: {}".format(arxiv_id))
    time.sleep(2.5)
    paper = sch.paper(
        "arXiv:{}".format(arxiv_id), timeout=10, include_unknown_references=True
    )
    try:
        return paper, [
            p["arxivId"] for p in paper["references"] if p["arxivId"] is not None
        ]
    except KeyError:
        return paper, []

def ciation_match(citations1: list, citations2: list):
    """Matches citations1 i with citations2"""

    pass