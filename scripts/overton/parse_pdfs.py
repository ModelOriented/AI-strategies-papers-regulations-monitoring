import os
from typing import List
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBoxHorizontal
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
import pandas as pd
import copy
import re
import string
import typer


def convert_pdf_to_paragraphs(path):
    document = open(path, 'rb')
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
    return paragraphs


def extract_text_from_pdf(pdfs_path: str, already_processed: List[str]):
    processed_files = {'file_id': [], 'paragraphs': []}
    pdfs = [f for f in os.listdir(pdfs_path) if f.endswith('.pdf')]
    successful_files = 0
    for pdf in pdfs:
        try:
            if pdf not in already_processed:
                print(f'Processing {pdf}', flush=True)
                with open(pdf, 'rb') as f:
                    pdf_content = f.read()
                pdf_text = convert_pdf_to_paragraphs(pdf_content)
                file_name = os.path.basename(pdf).split('.')[0]
                processed_files['file_id'].append(file_name)
                processed_files['paragraphs'].append(pdf_text)
                successful_files += 1
        except Exception as e:
            print(f'Error processing {pdf}', flush=True)
            # print(e, flush=True)

    processed_files = pd.DataFrame(processed_files)
    return processed_files, successful_files


def nwords(string):
    return len(string.split())


def paragraph_management(paragraphs, treshold=60):
    ls = copy.deepcopy(paragraphs)
    for i in range(len(ls)):  # document
        for j in range(len(ls[i])-1):  # paragraph
            if(nwords(ls[i][j]) < treshold):
                ls[i][j+1] = ls[i][j] + ' ' + ls[i][j+1]
                ls[i][j] = ''
        ls[i] = [s for s in ls[i] if s != '']
    return ls


def custom_regex(text):
    url = re.compile(r'(([a-z]{3,6}://)|(^|\s))([a-zA-Z0-9\-]+\.)+[a-z]{2,13}[\.\?\=\&\%\/\w\-]*\b([^@]|$)')
    whitespaces = re.compile(r'\n|\t')
    num = re.compile(r'[0-9]{1,30}')
    parenth = re.compile(r'\([0-9]{1,4}\)')
    email = re.compile(r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)')
    multiple_spaces = re.compile(r' {2,40}')
    html = re.compile(r'<.*?>')
    table = str.maketrans('', '', string.punctuation)
    marks = re.compile(r'•')

    text = whitespaces.sub(r' ', text)
    text = url.sub(r' ', text)
    text = email.sub(r' ', text)
    text = html.sub(r' ', text)
    text = parenth.sub(r' ', text)
    text = text.translate(table)
    text = num.sub(r' ', text)
    text = marks.sub(r' ', text)
    text = multiple_spaces.sub(r' ', text)
    return text


def clean(paragraphs):
    ls = copy.deepcopy(paragraphs)
    for i in range(len(ls)):
        for j in range(len(ls[i])):
            ls[i][j] = custom_regex(ls[i][j])
    return ls


def prepare_stats(lst):
    ls = copy.deepcopy(lst)
    n_pargraphs = []
    n_words = []
    for i in range(len(ls)):
        n_pargraphs.append(len(ls[i]))
        words = 0
        for j in range(len(ls[i])):
            words = words + nwords(ls[i][j])
        n_words.append(words)
    return (n_pargraphs, n_words)


def process(parquet_path: str, pdfs_path: str):
    df_exists = False
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
        print('Parquet file found, loading data...', flush=True)
        already_processed = df['file_id'].tolist()
        df_exists = True
    else:
        print('Parquet file not found, creating new one...', flush=True)
        already_processed = []

    print('Pdfs to process:', len(already_processed), flush=True)
    print('Extracting text from pdfs...', flush=True)
    df_new = extract_text_from_pdf(pdfs_path, already_processed)

    print('Merging paragraphs ...', flush=True)
    df_new['paragraphs'] = df_new['paragraphs'].apply(paragraph_management)

    print('Cleaning text...', flush=True)
    df_new['paragraphs'] = df_new['paragraphs'].apply(clean)

    print('Preparing statistics...', flush=True)
    df_new['stats'] = df_new['paragraphs'].apply(prepare_stats)

    if df_exists:
        print('Merging with existing data...', flush=True)
        df = pd.concat([df, df_new])
    else:
        df = df_new

    print('Saving data...', flush=True)
    df.to_parquet(parquet_path, index=False)

    print('Done!', flush=True)
    print('Final dataframe shape:', df.shape, flush=True)


if __name__ == '__main__':
    typer.run(process)