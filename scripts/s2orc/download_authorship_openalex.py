import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os

# import mars.utils
import json
import numpy as np
import multiprocessing
from tqdm import tqdm

OUT_PATH = os.path.join("data", "s2orc", "doi_to_authorship_big.json")


def batch(sequence, batch_size):
    return [
        sequence[i * batch_size : (i + 1) * batch_size]
        for i in range(len(sequence) // batch_size + 1)
    ]


def load_s2orc_prefiltered():
    return pd.read_parquet(
        "data/s2orc/big_ai_dataset.parquet",
        engine="pyarrow",
        columns=["doi"],
    )


def call_api(dois: list) -> list:
    url = "https://api.openalex.org/works?filter=doi:" + "|".join(dois)
    print(url)
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    response = session.get(url)

    if response.status_code != 200:
        raise Exception(response.status_code)

    response_json = response.json()
    return response_json["results"]


class MultithreadDataProcessing(multiprocessing.Process):
    def __init__(self, dois):
        multiprocessing.Process.__init__(self)
        self.dois = dois

    def run(self):
        get_affiliations(self.dois)


def get_affiliations(dois):
    # print("starting thread", os.getpid())
    i = 0
    errors = 0
    dois = [doi for doi in dois if not doi in doi_to_authorship]
    print("Will download:", len(dois))
    batches = batch(dois, 50)
    for doi_batch in tqdm(batches):
        try:
            response = call_api(doi_batch)
            for r in response:
                doi = r["doi"]
                doi_to_authorship[doi] = r["authorships"]
        except Exception as e:
            tqdm.write("Error: " + str(e))
            errors += 1
        i += 1
        if i % 100 == 0:
            with open(OUT_PATH, "w") as fp:
                json.dump(doi_to_authorship, fp)
            tqdm.write("Checkpoint saved!")


def run_parse(dois):
    no_of_threads = multiprocessing.cpu_count()
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


if __name__ == "__main__":
    doi_to_authorship = {}
    failed_doi = {}
    try:
        f = open(OUT_PATH, "r")
        doi_to_authorship = json.load(f)
        f.close()
    except FileNotFoundError:
        print("No checkpoint file found!")
    df = load_s2orc_prefiltered()
    dois = df["doi"].unique()
    get_affiliations(dois)
    # run_parse(dois)
    print("Saving results...")
    with open(OUT_PATH, "w") as fp:
        json.dump(doi_to_authorship, fp)
    print("Done!")