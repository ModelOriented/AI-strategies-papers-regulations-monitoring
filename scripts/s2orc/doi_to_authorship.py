import json

import os
from glob import glob
from tqdm import tqdm

def doi_to_authorship(directory_path: str):    
    doi_to_authorship = {}

    for file_path in tqdm(glob(os.path.join(directory_path+"/*/*"))):
        with open(file_path, "r") as fp:
            for line in fp.readlines():
                oa = json.loads(line)
                if oa['doi'] is not None:
                    doi_to_authorship[oa['doi']] = oa['authorships']

    return doi_to_authorship

if __name__ == "__main__":
    doi_dict = doi_to_authorship('/raid/shared/openalex/openalex-snapshot/data/works')
    with open('data/s2orc/doi_to_authorship_extended.json', 'w') as fp:
        json.dump(doi_dict, fp)