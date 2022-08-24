
import re
import string
import copy
import pandas as pd
import requests
import os
import PyPDF2
import typer
from joblib import Parallel, delayed

from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBoxHorizontal
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage

UNOPENABLE_PDFS_PATH = 'unopenable.csv'
PROGRESS = 'progress.txt'
MISSING = 'missing.txt'
TO_DOWNLOAD = 'to_download.txt'
TEXTS_FILE_NAME = "text_col.parquet"


PDF_URL_COLNAME = 'pdf_url'
TITLE_COLNAME = 'title'
DOC_ID_COLNAME = 'pdf_document_id'
PDF_FILENAME_COLNAME = 'pdf_filename'
TEXT_COLNAME = 'text'

def download_parallel(pdfs_path, data, n_jobs):
    try:
        open(PROGRESS, 'x')
        open(MISSING, 'x')
        open(TO_DOWNLOAD, 'x')
    except:
        pass
    with open(PROGRESS, 'r') as f:  # check if we started process, continue after restart from last downloaded pdf
        line = f.readline()
        if (len(line) == 0):
            print("Starting the download...")
        else:
            print("Resuming the download...")
    fill = False
    with open(TO_DOWNLOAD, 'r') as f:  # if it's empty, populate the to_download
        if (len(f.readline()) == 0):
            fill = True

    if fill == True:  # if it's empty, populate the to_download
        with open(TO_DOWNLOAD, 'a') as f:
            for s in data[DOC_ID_COLNAME]:
                f.writelines(s+'\n')

    if fill == False:  # if there is some content, compare progress and to_download
        with open("to_download.txt", "r") as f:
            to_download_lines = f.readlines()
        with open(PROGRESS, "r") as f:
            progress_lines = f.readlines()

        if len(to_download_lines) == len(progress_lines):  # in case of finishing
            print('We have downloaded everything')
            return 0
        downloaded_pdfs = os.listdir(pdfs_path)
        downloaded_pdfs = [l[:-4] for l in downloaded_pdfs]
        with open("to_download.txt", "w") as f:
            for line in to_download_lines:
                # if line not in progress_lines:
                if line not in downloaded_pdfs:
                    f.writelines(line)
    Parallel(n_jobs=n_jobs)(delayed(download_pdf)(pdfs_path, data, i) for i in range(len(data)))

def download_pdf(pdfs_dir, data, i):
    with open(TO_DOWNLOAD, 'r') as file:  # if i-th file is not in to_download, go for the next one
        if str(data[DOC_ID_COLNAME][i]+'\n') not in file.readlines():
            return 0
    try:
        url = data[PDF_URL_COLNAME][i]
        r = requests.get(url, allow_redirects=True)
        pdf_path = pdfs_dir + '/' + str(data[DOC_ID_COLNAME][i]) + '.pdf'
        open(pdf_path, 'wb').write(r.content)

        content_type = str(r.headers.get('Content-Type'))
        if not str(content_type).startswith('application/pdf'):
            print("Warning: wrong content type", content_type, "| URL:", url)
        with open(PROGRESS, 'a') as f:  # write title to progress
            f.writelines(str(data[DOC_ID_COLNAME][i])+'\n')

    except:
        url = data[PDF_URL_COLNAME][i]
        if(url == 'nan'):
            with open(MISSING, 'a') as f:  # write title to missing
                f.writelines(str(data[DOC_ID_COLNAME][i])+'\n')

            with open(PROGRESS, 'a') as f:  # write title to progress
                f.writelines(str(data[DOC_ID_COLNAME][i])+'\n')

def convert_pdf_to_paragraphs(path):
    """
    Desc:   converts pdf lsit of strings - paragraphs
    Input:  path to the pdf file
    Output: list of strings contatining text of the given paragraph
    """
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

def convert_pdf_folder_to_paragraphs(path, project_path, names):
    """
    Desc:   Converts all pdf files in the given path to paragraphs and then saves them as 2 pickles of lists.
    Input:  path to the folder contatining pdfs
            initial_path to the folder of project
            names list of string with names of saved pickles
    Output: pickled list of lists with paragraphs per document
            pickled list of doc names
    """
    list_of_lists = []
    names = []

    listdir = os.listdir(path)
    try:
        df = pd.read_parquet(os.path.join(str(project_path), TEXTS_FILE_NAME))
        print('Loaded previous dataframe of length', len(df))
    except:
        df = pd.DataFrame(list(zip(names, list_of_lists)), columns=[PDF_FILENAME_COLNAME, TEXT_COLNAME, ])
        print('Created new dataframe')
    for filename in listdir:
        if filename in df[PDF_FILENAME_COLNAME].values:
            print("Already processed:", filename)
        else:
            try:
                if filename.endswith(".pdf"):
                    file_path = f"{path}/{filename}"
                    print(f"Processing: {file_path}...",flush=True)
                    names.append(filename)
                    list_of_paragraphs = convert_pdf_to_paragraphs(file_path)
                    list_of_lists.append(list_of_paragraphs)
                    df.loc[-1] = [filename, list_of_paragraphs]  # adding a row
                    df.index = df.index + 1  # shifting index
                    df = df.sort_index()  # sorting by index
                else:
                    print(f"File not pdf: {filename}", flush=True)
            except Exception as e:
                print(f"Error during processing: {filename}",str(e))
                df.to_parquet(os.path.join(str(project_path), TEXTS_FILE_NAME))

    df.to_parquet(os.path.join(str(project_path),TEXTS_FILE_NAME))

def remove_corrupted_files(path):
    i = 0
    corrupted = []
    for file in os.listdir(path):
        try:
            pdfObj = open(os.path.join(path, file), 'rb')
            doc = PyPDF2.PdfFileReader(pdfObj)
            doc.numPages
            pdfObj.close()
        except Exception as e:
            i += 1
            pdfObj.close()
            print("Unopenable file:", file, flush=True)
            print("Exception:", str(e), flush=True)
            corrupted.append(file)
            # os.remove(os.path.join(path, file))
    print(str(i) + " Files unopenable", flush=True)
    pd.Series(corrupted).to_csv(UNOPENABLE_PDFS_PATH)

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

def clean(lst):
    ls = copy.deepcopy(lst)
    for i in range(len(ls)):
        for j in range(len(ls[i])):
            ls[i][j] = custom_regex(ls[i][j])
    return ls

def nwords(string):
    return len(string.split())

def paragraph_management(df, treshold=60):
    ls = copy.deepcopy(df[TEXT_COLNAME])
    for i in range(len(ls)):  # document
        for j in range(len(ls[i])-1):  # paragraph
            if(nwords(ls[i][j]) < treshold):
                ls[i][j+1] = ls[i][j] + ' ' + ls[i][j+1]
                ls[i][j] = ''
        ls[i] = [s for s in ls[i] if s != '']
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
    return n_pargraphs, n_words

def merge_tables(meta, subtable):
    huge_table = copy.deepcopy(meta)
    huge_table['Name'] = [None]*len(huge_table)
    huge_table[TEXT_COLNAME] = [None]*len(huge_table)
    huge_table['n_paragraphs'] = [None]*len(huge_table)
    huge_table['n_words'] = [None]*len(huge_table)
    j=1
    for i in range(len(subtable)):
        # idx = int(subtable['Name'][i].strip('.pdf'))
        # idx = huge_table.index[subtable['Name'] == huge_table['Name'][i].strip('.pdf')].tolist()
        try:
            temp_name = re.sub(r'\.pdf$', '', subtable['Name'][i])
            idx = huge_table.index[huge_table[DOC_ID_COLNAME] == temp_name].tolist()[0]
            huge_table['Name'][idx] = subtable['Name'][i]
            huge_table[TEXT_COLNAME][idx] = subtable[TEXT_COLNAME][i]
            huge_table['n_paragraphs'][idx] = subtable['n_paragraphs'][i]
            huge_table['n_words'][idx] = subtable['n_words'][i]
        except Exception as e:
            print(e)
            j += 1

    return huge_table


def main(pdfs_path: str, project_path: str, overton_table_path: str, n_jobs: int):
    """
    pdfs_path           : path to pdf folder
    project_path        : path to script folder
    overton_table_path  : path to overton table with pdf links and meta data
    n_jobs              : int, number of jobs for downloading
    """
    # Process
    os.chdir(project_path)
    print('Preparing file structre...')
    os.makedirs(pdfs_path, exist_ok=True)

    print('Reading dump file...', flush=True)
    dump_df = pd.read_parquet(overton_table_path)

    print("Downloading pdfs...", flush=True)
    missing = download_parallel(pdfs_path, dump_df, n_jobs=n_jobs)
    print(str(missing) + ' files are missing')

    print('Removing corrupted files')
    remove_corrupted_files(pdfs_path)

    print("Converting to raw paragraphs...", flush=True)
    names = ['paragraphs_for_pdfs', 'pdf_internal_name']
    convert_pdf_folder_to_paragraphs(pdfs_path, project_path, names)

    print("Loading paragraphs files...", flush=True)
    paragraph_df = pd.read_parquet(os.path.join(str(project_path),TEXTS_FILE_NAME))

    print("Merging paragraphs...")
    merged_paragraphs = paragraph_management(paragraph_df)

    print("Cleaning paragraphs...")
    clean_paragraphs = clean(merged_paragraphs)

    print("Preparing statistics...")
    np, nw = prepare_stats(clean_paragraphs)

    print("Creating subtable...", flush=True)

    texts_table = pd.DataFrame(list(zip(paragraph_df[PDF_FILENAME_COLNAME], clean_paragraphs, np, nw)), columns=[
                            PDF_FILENAME_COLNAME, TEXT_COLNAME, 'n_paragraphs', 'n_words'])

    print('Reading dump file...')
    dump_df = pd.read_parquet(overton_table_path)

    print('Preparing final table...')
    # final_table = merge_tables(data, subtable)
    texts_table[DOC_ID_COLNAME] = texts_table[PDF_FILENAME_COLNAME].apply(lambda x: x[:-4])

    final_table = pd.merge(dump_df, texts_table, on=DOC_ID_COLNAME)
    print('Saving final table...')
    final_table.to_parquet(os.path.join(str(project_path),"processed.parquet"), index=False)


if __name__ == "__main__":
    typer.run(main)