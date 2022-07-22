from tqdm import tqdm
import os

import joblib
import pandas as pd
import PyPDF2
import requests
import typer
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBoxHorizontal
from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.pdfpage import PDFPage

PROGRES_FILE = 'progress.txt'
MISSING_FILE = 'missing.txt'

def download(pdfs_path:str, data):
    try:
        open(PROGRES_FILE, 'x')
        open(MISSING_FILE, 'x')
    except:
        pass
    with open(PROGRES_FILE, 'r') as f: # check if we started process, continue after restart from last downloaded pdf
        line = f.readline()
        try:
            k = int(line)
            print("Resuming the download from", str(k), "...")
        except:
            with open(PROGRES_FILE, 'w') as ff:
                ff.write('0')
                k = 0
    with open(MISSING_FILE, 'w') as f:
        f.write(str(0))

    for i in tqdm(list(range(k, len(data)))): # try downloading pdfs
        try:
            url = data['PDF URL'][i]
            r = requests.get(url, allow_redirects=True)
            st = pdfs_path + '/' + str(i) + '.pdf'
            open(st, 'wb').write(r.content)

            with open(PROGRES_FILE, 'w') as f: # actualize progress
                f.write(str(i))
        except:
            url = data['PDF URL'][i]
            if(url == 'nan'):
                with open(MISSING_FILE, 'r') as f: # actualize number of missing pdfs
                    miss = int(f.readline())
                    with open(MISSING_FILE, 'w') as ff:
                        ff.write(str(miss+1))
            
            with open(PROGRES_FILE, 'w') as f: # actualize progress
                f.write(str(i))

    with open(MISSING_FILE, 'r') as f: # read, how many pdfs are missing
        missing = int(f.readline())
        print("Download finished with", str(missing), "missing urls")


def convert_pdf_to_paragraphs(path):
    """
    Desc:   converts pdf lsit of strings - paragraphs
    Input:  path to the pdf file
    Output: list of strings contatining text of the given paragraph
    """
    document = open(path, 'rb')
    #Create resource manager
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


def convert_pdf_folder_to_paragraphs(path,initial_path,w_path,names): # No method that preserves the process when it is corrupted. TO DO
    """
    Desc:   Converts all pdf files in the given path to paragraphs and then saves them as 2 pickles of lists.
    Input:  path to the folder contatining pdfs
            initial_path to the folder of project
            w_path to the folder wehere txt files are saved
            names list of string with names of saved pickles
    Output: pickled list of lists with paragraphs per document
            pickled list of doc names
    """
    list_of_lists = []
    names = []
    os.chdir(path)
    for file in os.listdir():
        if file.endswith(".pdf"):
            file_path = f"{path}/{file}"
        print(file_path)
        names.append(file)
        os.chdir(initial_path)
        ls = convert_pdf_to_paragraphs(file_path)
        list_of_lists.append(ls)

    os.chdir(initial_path)

    joblib.dump(list_of_lists,w_path + "/" + names[0])
    joblib.dump(names,w_path + "/" + names[1])


def remove_corrupted_files(path):
    i = 0
    os.chdir(path)
    for file in os.listdir():
        try:
            pdfObj = open(file, 'rb')
            doc = PyPDF2.PdfFileReader(pdfObj)
            doc.numPages
        except:
            i += 1
            pdfObj.close()
            os.remove(file)
    os.chdir('..')
    print(str(i), "Files removed")

def main(overton_table_path: str, pdfs_path: str='pdfs',txt_path = 'texts', project_path: str='.'):
    """
    pdfs_path           : path to pdf folder
    project_path        : path to script folder
    txt_path            : path where txt joblib dumps are stored
    overton_table_path  : path to overton table with pdf links and meta data
    """
    # Process
    os.chdir(project_path)
    print('Preparing file structre...')
    # try:
    os.makedirs(pdfs_path, exist_ok=True)
    os.makedirs(txt_path, exist_ok=True)
    # except:
    #     print('Structure already exists!')

    print('Reading XLS file...')
    data = pd.read_excel(overton_table_path)

    print("Downloading pdfs...")
    missing = download(pdfs_path,data)
    print(str(missing) +' files are missing')

    print('Removing corrupted files')
    remove_corrupted_files(pdfs_path)

    print("Converting to raw paragraphs...")
    names = ['paragraphs_for_pdfs','pdf_internal_name']
    convert_pdf_folder_to_paragraphs(pdfs_path,project_path,txt_path,names)

if __name__=="__main__":
    typer.run(main)
