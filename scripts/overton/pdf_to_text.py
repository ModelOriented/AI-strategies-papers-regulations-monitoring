import os
from joblib import Parallel, delayed
import time
import typer
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBoxHorizontal
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
import json

def read_pdfs_from_directory(pdfs_path: str):
    files = os.listdir(pdfs_path)
    files = [f.split('.pdf')[0] for f in files if f.endswith('.pdf')]
    return files


def read_jsons_from_directory(txt_path: str):
    files = os.listdir(txt_path)
    files = [f.split('.json')[0] for f in files if f.endswith('.json')]
    return files


def read_failed_to_convert_files(file_path: str):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            lines = f.readlines()
    else:
        lines = []
        print(f'File {file_path} does not exist, creating one ...', flush=True)
        with open(file_path, 'w') as f:
            f.write('')
    return lines


def extract_text_from_pdf(filename: str, pdf_path: str, jsons_path: str, failed_to_convert_path: str, i: int, n_files: int):
    try:
        document = open(os.path.join(pdf_path, filename) + '.pdf', 'rb')
        # Create resource manager
        rsrcmgr = PDFResourceManager()
        # Set parameters for analysis.
        laparams = LAParams()
        # Create a PDF page aggregator object.
        device = PDFPageAggregator(rsrcmgr, laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        paragraphs = []
        for page in PDFPage.get_pages(document):
            interpreter.process_page(page)
            # receive the LTPage object for the page.
            layout = device.get_result()
            for element in layout:
                if isinstance(element, LTTextBoxHorizontal):
                    paragraphs.append(element.get_text())

        # Saving to json
        dictionary = {'paragraphs': paragraphs}
        json_object = json.dumps(dictionary)

        with open(os.path.join(jsons_path, filename) + '.json', 'w') as f:
            f.write(json_object)
        print(f'Processed {filename}, {i} / {n_files}, at {time.strftime("%d/%m/%Y %H:%M:%S")}', flush=True)
    except Exception as e:
        print(f'Failed to process {filename}', flush=True)
        with open(failed_to_convert_path, 'a') as f:
            f.write(filename + '\n')
        print(f'Failed to process {filename} at {time.strftime("%d/%m/%Y %H:%M:%S")}', flush=True)
        print(e, flush=True)


def process(pdfs_path: str, txt_path: str, failed_to_convert_path: str, jb_n_jobs: int):
    print('Looking for pdfs in directory', flush=True)
    pdfs = read_pdfs_from_directory(pdfs_path)
    print(f'Found {len(pdfs)} pdfs', flush=True)

    print('Looking for txts in directory', flush=True)
    txts = read_jsons_from_directory(txt_path)
    print(f'Found {len(txts)} jsons', flush=True)

    print('Looking for failed to convert files', flush=True)
    failed_to_convert = read_failed_to_convert_files(failed_to_convert_path)
    print(f'Found {len(failed_to_convert)} failed to convert files', flush=True)

    files_to_process = [f for f in pdfs if f not in txts and f not in failed_to_convert]
    print(f'Found {len(files_to_process)} files to process', flush=True)

    print('Extracting text from pdfs with multiprocessing', flush=True)
    Parallel(n_jobs=jb_n_jobs)(delayed(extract_text_from_pdf)(f, pdfs_path, txt_path, failed_to_convert_path, i, len(files_to_process)) for i, f in enumerate(files_to_process))
    print('Done', flush=True)


if __name__ == '__main__':
    typer.run(process)

