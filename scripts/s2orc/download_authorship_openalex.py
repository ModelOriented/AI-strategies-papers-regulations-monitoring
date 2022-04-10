import pandas as pd
import requests
import os
import mars.utils
import json
from tqdm import tqdm

ROOT_DIR = mars.utils.ROOT_DIR
FILE_DIR = os.path.join(ROOT_DIR, 'data', 's2orc', 'doi_to_authorship.json')

def load_s2orc_prefiltered():
    print(os.path.join(ROOT_DIR, 'data/s2orc/s2orc_ai_prefiltered.csv'))
    return pd.read_csv(os.path.join(ROOT_DIR, 'data/s2orc/s2orc_ai_prefiltered.csv'))


def get_authorship(doi):
    if pd.isnull(doi):
        return []
    response = requests.get("https://api.openalex.org/works/doi:" + str(doi))
    if response.status_code != 200:
        return response.status_code
    response_json = response.json()
    try:
        return response_json['authorships']
    except KeyError:
        return []


df = load_s2orc_prefiltered()
doi_to_authorship = {}

try:
    f = open(FILE_DIR, 'r')
    doi_to_authorship = json.load(f)
    f.close()
except FileNotFoundError:
    pass

starting_index = len(doi_to_authorship)

for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    if row['doi'] in doi_to_authorship:
        continue
    authorship = get_authorship(row['doi'])
    doi_to_authorship[row['doi']] = authorship
    if index % 100 == 0:
        with open(os.path.join(ROOT_DIR, 'data/s2orc/doi_to_authorship.json'), 'w') as fp:
            json.dump(doi_to_authorship, fp)
