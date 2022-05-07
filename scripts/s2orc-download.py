import os
import subprocess
import gzip
import io
import json
from tqdm import tqdm


# process single batch
def process_batch(batch: dict):
    # this downloads both the metadata & full text files for a particular shard
    cmd = ["wget", "-O", batch['input_metadata_path'], batch['input_metadata_url']]
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
#przepisać na curl, lub co kolwiek w pythonie
    cmd = ["wget", "-O", batch['input_pdf_parses_path'], batch['input_pdf_parses_url']]
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)

    # first, let's filter metadata JSONL to only papers with a particular field of study.
    # we also want to remember which paper IDs to keep, so that we can get their full text later.
    paper_ids_to_keep = set()
    with gzip.open(batch['input_metadata_path'], 'rb') as gz, open(batch['output_metadata_path'], 'wb') as f_out:
        f = io.BufferedReader(gz)
        for line in tqdm(f.readlines()):
            metadata_dict = json.loads(line)
            paper_id = metadata_dict['paper_id']
            mag_field_of_study = metadata_dict['mag_field_of_study']
            if mag_field_of_study and 'Medicine' in mag_field_of_study:     # TODO: <<< change this to your filter
                paper_ids_to_keep.add(paper_id)
                f_out.write(line)

    # now, we get those papers' full text
    with gzip.open(batch['input_pdf_parses_path'], 'rb') as gz, open(batch['output_pdf_parses_path'], 'wb') as f_out:
        f = io.BufferedReader(gz)
        for line in tqdm(f.readlines()):
            metadata_dict = json.loads(line)
            paper_id = metadata_dict['paper_id']
            if paper_id in paper_ids_to_keep:
                f_out.write(line)

    # now delete the raw files to clear up space for other shards
    os.remove(batch['input_metadata_path'])
    os.remove(batch['input_pdf_parses_path'])


if __name__ == '__main__':

    METADATA_INPUT_DIR = 'metadata/raw/'
    METADATA_OUTPUT_DIR = 'metadata/medicine/'
    PDF_PARSES_INPUT_DIR = 'pdf_parses/raw/'
    PDF_PARSES_OUTPUT_DIR = 'pdf_parses/medicine/'

    os.makedirs(METADATA_INPUT_DIR, exist_ok=True)
    os.makedirs(METADATA_OUTPUT_DIR, exist_ok=True)
    os.makedirs(PDF_PARSES_INPUT_DIR, exist_ok=True)
    os.makedirs(PDF_PARSES_OUTPUT_DIR, exist_ok=True)

    # TODO: make sure to put the links we sent to you here
    # there are 100 shards with IDs 0 to 99. make sure these are paired correctly.
    download_linkss = [#todo movve to gitignored
        {"metadata": "https://...", "pdf_parses": "https://..."},  # for shard 0
        {"metadata": "https://...", "pdf_parses": "https://..."},  # for shard 1
        {"metadata": "https://...", "pdf_parses": "https://..."},  # for shard 2
    ]

    # turn these into batches of work
    # TODO: feel free to come up with your own naming convention for 'input_{metadata|pdf_parses}_path'
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

    for batch in batches:
        process_batch(batch=batch)
