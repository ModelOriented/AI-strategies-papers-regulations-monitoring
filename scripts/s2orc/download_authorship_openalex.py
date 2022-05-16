import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os
import mars.utils
import json
from tqdm import tqdm
import numpy as np
import multiprocessing

ROOT_DIR = mars.utils.ROOT_DIR
FILE_DIR = os.path.join(ROOT_DIR, 'data', 's2orc', 'doi_to_authorship_big_dataset.json')

def load_s2orc_prefiltered():
    print(os.path.join(ROOT_DIR, 'data/s2orc/big_ai_dataset.parquet'))
    return pd.read_parquet(os.path.join(ROOT_DIR, 'data/s2orc/big_ai_dataset.parquet'), engine='pyarrow', columns=['doi'])


def get_response_json(doi):
    if pd.isnull(doi):
        return []

    url = "https://api.openalex.org/works/doi:" + str(doi)
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url)

    if response.status_code != 200:
        return response.status_code
    response_json = response.json()
    return response_json

class MultithreadDataProcessing(multiprocessing.Process):
    def __init__(self, dois):
        multiprocessing.Process.__init__(self)
        self.dois = dois

    def run(self):
        get_affiliations(self.dois)


def get_affiliations(dois):
    print('starting thread', os.getpid())
    i = 0
    for doi in dois:
        if doi in doi_to_authorship:
            continue

        response_json = get_response_json(doi)
        if response_json != []:
            doi_to_authorship[doi] = response_json

        response_json = get_response_json(doi)
        if type(response_json) != int:
            if 'authorships' in response_json:
                doi_to_authorship[doi] = response_json['authorships']
            else:
                doi_to_authorship[doi] = []

        # print(f'{os.getpid()}: {i} / {len(dois)}')
        i += 1


def run_parse():
    df = load_s2orc_prefiltered()
    no_of_threads = multiprocessing.cpu_count()
    dois = df['doi'].unique()
    number_of_dois = len(dois)
    chunk_size = divmod(number_of_dois, no_of_threads)[0]
    split_array = list(range(chunk_size, chunk_size * no_of_threads, chunk_size))

    dois_chunks = np.split(dois, split_array)
    threads = []

    for chunk in dois_chunks:
        t = MultithreadDataProcessing(chunk)
        threads.append(t)

    for thread in threads:
        thread.start()

    print("Finished main thread")


if __name__ == '__main__':
    doi_to_authorship = {}
    # doi_to_concepts = {}
    # doi_to_counts = {}
    try:
        f = open(FILE_DIR, 'r')
        doi_to_authorship = json.load(f)
        f.close()
    except FileNotFoundError:
        pass

    run_parse()
    with open(os.path.join(ROOT_DIR, 'data/s2orc/doi_to_authorship_big_dataset.json'), 'w') as fp:
        json.dump(doi_to_authorship, fp)


    # if 'concepts' in response_json:
    #     doi_to_concepts[row['doi']] = response_json['concepts']
    # else:
    #     doi_to_concepts[row['doi']] = []
    #
    # if 'counts_by_year' in response_json:
    #     doi_to_counts[row['doi']] = response_json['counts_by_year']
    # else:
    #     doi_to_counts[row['doi']] = []