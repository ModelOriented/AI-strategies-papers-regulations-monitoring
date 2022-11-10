import json

import os

def doi_to_authorship(directory_path):

    directory = os.fsencode(directory_path)
    
    doi_to_authorship = {}

    for update_dir in os.listdir(directory):
        for file in os.listdir(update_dir):

            oa_file = open(file)
            oa = json.load(oa_file)

            for key in oa:
                doi_to_authorship[key['doi']] = key['author']
    return doi_to_authorship

if __name__ == "__main__":
    doi_dict = doi_to_authorship('/raid/shared/openalex/openalex-snapshot/data/works')
    with open('data/s2orc/doi_to_authorship_extended.json', 'w') as fp:
        json.dump(doi_dict, fp)