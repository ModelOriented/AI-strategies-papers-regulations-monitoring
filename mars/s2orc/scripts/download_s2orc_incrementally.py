"""
Creates directory structure:

|-- metadata/
    |-- raw/
        |-- metadata_0.jsonl.gz      << input; deleted after processed
    |-- ai/
        |-- metadata_0.jsonl         << output
|-- pdf_parses/
    |-- raw/
        |-- pdf_parses_0.jsonl.gz    << input; deleted after processed
    |-- a/
        |-- pdf_parses_0.jsonl       << output

"""


import os
import subprocess
import gzip
import io
import json
import re
from tqdm import tqdm
from patterns import *


# process single batch
def process_batch(batch: dict, matcher):
    # this downloads both the metadata & full text files for a particular shard
    cmd = ["wget", "-O", batch['input_metadata_path'], batch['input_metadata_url']]
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    papers_without_abstract=0

    # first, let's filter metadata JSONL to only papers with a particular field of study.
    # we also want to remember which paper IDs to keep, so that we can get their full text later.
    paper_ids_to_keep = set()
    papers = 0

    with gzip.open(batch['input_metadata_path'], 'rb') as gz, open(batch['output_metadata_path'], 'wb') as f_out:

        f = io.BufferedReader(gz)
        for line in tqdm(f.readlines()):
            papers += 1
            metadata_dict = json.loads(line)
            paper_id = metadata_dict['paper_id']
            text =  metadata_dict['title']
            if metadata_dict['abstract'] !=None:
                met_abstract = metadata_dict['abstract'].replace("\n", " ")
                text =  text + ' ' + met_abstract
            else:
                papers_without_abstract += 1
            if matcher.search(text)!= None:     # TODO: <<< change this to your filter
                paper_ids_to_keep.add(paper_id)
                f_out.write(line)

    cmd = ["wget", "-O", batch['input_pdf_parses_path'], batch['input_pdf_parses_url']]
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)

    # now, we get those papers' full text
    with gzip.open(batch['input_pdf_parses_path'], 'rb') as gz, open(batch['output_pdf_parses_path'], 'wb') as f_out:
        f = io.BufferedReader(gz)
        for line in tqdm(f.readlines()):
            metadata_dict = json.loads(line)
            paper_id = metadata_dict['paper_id']
            if paper_id in paper_ids_to_keep:
                f_out.write(line)

    os.remove(batch['input_metadata_path'])
    os.remove(batch['input_pdf_parses_path'])

    return (papers_without_abstract, papers)

if __name__ == '__main__':

    METADATA_INPUT_DIR = 'metadata/raw/'
    METADATA_OUTPUT_DIR = '../metadata/ai/'
    PDF_PARSES_INPUT_DIR = 'pdf_parses/raw/'
    PDF_PARSES_OUTPUT_DIR = '../pdf_parses/ai/'

    os.makedirs(METADATA_INPUT_DIR, exist_ok=True)
    os.makedirs(METADATA_OUTPUT_DIR, exist_ok=True)
    os.makedirs(PDF_PARSES_INPUT_DIR, exist_ok=True)
    os.makedirs(PDF_PARSES_OUTPUT_DIR, exist_ok=True)

    # make sure to put the links s2orc sent to you here
    # there are 100 shards with IDs 0 to 99. make sure these are paired correctly.
    download_linkss = [
        {'metadata': '<link to metadata>','pdf_parses': '<link pdfs>'},
        ]

    batches = [{
        'input_metadata_url': download_links['metadata'],
        'input_metadata_path': os.path.join(METADATA_INPUT_DIR,
                                            os.path.basename(download_links['metadata'].split('?')[0])),
        'output_metadata_path': os.path.join(METADATA_OUTPUT_DIR,
                                             os.path.basename(download_links['metadata'].split('?')[0])),
        'input_pdf_parses_url': download_links['pdf_parses'],
        'input_pdf_parses_path': os.path.join(PDF_PARSES_INPUT_DIR,
                                              os.path.basename(download_links['pdf_parses'].split('?')[0])),
        'output_pdf_parses_path': os.path.join(PDF_PARSES_OUTPUT_DIR,
                                               os.path.basename(download_links['pdf_parses'].split('?')[0])),
    } for download_links in download_linkss]
    (papers_without_abstract, papers) = (0,0)
    matcher = re.compile('|'.join(AI_PAPER_PATTERNS))
    for batch in batches:
        (papers_without_abstract_b, papers_b) = process_batch(batch=batch, matcher=matcher)
        papers_without_abstract+=papers_without_abstract_b
        papers+=papers_b
    print(papers_without_abstract, papers)
