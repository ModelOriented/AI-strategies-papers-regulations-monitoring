import pandas as pd
import requests
import os
import typer
from joblib import Parallel, delayed
import time
from typing import Tuple

PDF_URL = 'pdf_url'
PDF_FILENAME_COLNAME = 'pdf_document_id'

def get_already_downloaded(pdfs_path: str):
    already_downloaded = []
    if os.path.exists(pdfs_path):
        for subdir, dirs, files in os.walk(pdfs_path):
            for file in files:
                already_downloaded.append(file.split(".")[0])
    else:
        raise Exception('Path does not exist:', pdfs_path)
    return already_downloaded


def get_unable_to_download(failed_to_download_path: str):
    unable_to_download = []
    if os.path.exists(failed_to_download_path):
        with open(failed_to_download_path, 'r') as f:
            unable_to_download = [line.rstrip for line in f]
    else:
        with open(failed_to_download_path, 'w') as f:
            pass
    return unable_to_download


def get_all_files(dump_path: str):
    all_files = []
    if os.path.exists(dump_path):
        df = pd.read_parquet(dump_path)
        df.dropna(subset=[PDF_URL], inplace=True)
        all_files_name = df[PDF_FILENAME_COLNAME].tolist()
        print('All files:', len(all_files_name), flush=True)
        all_files_ulr = df[PDF_URL].tolist()
    else:
        raise Exception('Path does not exist:', dump_path)
    for name, url in zip(all_files_name, all_files_ulr):
        all_files.append((name, url))
    return all_files


def download_pdf(file_names: Tuple[str, str], output_path: str, failed_to_download_path: str):
    filename, url = file_names
    file_path = os.path.join(output_path, filename)
    if not os.path.exists(file_path + '.pdf'):
        try:
            r = requests.get(url, allow_redirects=True)
            with open(file_path + '.pdf', 'wb') as f:
                f.write(r.content)

            content_type = str(r.headers.get('Content-Type'))
            if not str(content_type).startswith('application/pdf'):
                print("Warning: wrong content type", content_type, "| file:", file_names[0],
                      "on ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), flush=True)
        except:
            with open(failed_to_download_path, 'a') as f:
                f.write(filename + '\n')
            print('Unable to download:', filename, flush=True)
    else:
        print('Already downloaded:', filename, flush=True)


def run(pdfs_path: str, failed_to_download_path: str, dump_path: str):
    # Checking for already downloaded files
    print('Checking for already downloaded files...', flush=True)
    already_downloaded = get_already_downloaded(pdfs_path)
    print('Already downloaded:', len(already_downloaded), flush=True)

    # Checking for files we were not able to download
    print('Checking for files we were not able to download...', flush=True)
    unable_to_download = get_unable_to_download(failed_to_download_path)
    print('Unable to download:', len(unable_to_download), flush=True)

    # Reading the list of files to download
    print('Reading the list of files to download...', flush=True)
    # all_files = (pdf_name, pdf_url)
    all_files = get_all_files(dump_path)
    remaining_to_download = [x for x in all_files if (x[0] not in already_downloaded and x[0] not in unable_to_download)]
    print('Remaining to download:', len(remaining_to_download), flush=True)

    # Downloading the files
    print('Downloading the files...', flush=True)
    Parallel(n_jobs=10)(delayed(download_pdf)(pdf, pdfs_path, failed_to_download_path) for pdf in remaining_to_download)

    # Checking for files we were not able to download
    print('Checking for files we were not able to download...', flush=True)
    unable_to_download = get_unable_to_download(failed_to_download_path)
    print('Unable to download:', len(unable_to_download), flush=True)


if __name__ == '__main__':
    typer.run(run)
