import re
import string
import copy
import pandas as pd
import requests
import os
import PyPDF2
import joblib
import typer

from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBoxHorizontal
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage

def main(pdfs_path : str,project_path : str,txt_path : str,overton_table_path: str):
    """
    pdfs_path           : path to pdf folder
    project_path        : path to script folder
    txt_path            : path where txt joblib dumps are stored
    overton_table_path  : path to overton table with pdf links and meta data
    """
    def download(pdfs_path,data):
        try:
            open('progress.txt', 'x')
            open('missing.txt', 'x')
        except:
            pass
        with open('progress.txt', 'r') as f: # check if we started process, continue after restart from last downloaded pdf
            line = f.readline()
            try:
                k = int(line)
                print("Resuming the download from " + str(k) + " ...")
            except:
                with open('progress.txt', 'w') as ff:
                    ff.write('0')
                    k = 0
        with open('missing.txt', 'w') as f:
            f.write(str(0))

        for i in range(k, len(data)): # try downloading pdfs
            try:
                url = data['Document URL'][i]
                r = requests.get(url, allow_redirects=True)
                st = pdfs_path + '/' + str(i) + '.pdf'
                open(st, 'wb').write(r.content)

                with open('progress.txt', 'w') as f: # actualize progress
                    f.write(str(i))
            except:
                url = data['Document URL'][i]
                if(url == 'nan'):
                    with open('missing.txt', 'r') as f: # actualize number of missing pdfs
                        miss = int(f.readline())
                        with open('missing.txt', 'w') as ff:
                            ff.write(str(miss+1))
                
                with open('progress.txt', 'w') as f: # actualize progress
                    f.write(str(i))

        with open('missing.txt', 'r') as f: # read, how many pdfs are missing
            missing = int(f.readline())
            print("Download finished with " + str(missing) + " missing urls")
    
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

    def convert_pdf_folder_to_paragraphs(path,project_path,names):
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
        os.chdir(path)

        df = pd.DataFrame(list(zip(names,list_of_lists)),columns = ['Name','Text',])
        listdir = os.listdir()
        try:
            df = pd.read_parquet(str(project_path) + "/Text_col.parquet")
            k = len(df) - 1
            print('Loaded previous dataframe')
            print(df)
        except:
            print('Created new dataframe')
            k = 0
        for i in range(k,len(listdir)):
            try:
                file = listdir[i]
                if file.endswith(".pdf"):
                    file_path = f"{path}/{file}"
                print(file_path)
                names.append(file)
                os.chdir(project_path)
                ls = convert_pdf_to_paragraphs(file_path)
                list_of_lists.append(ls)

                df.loc[-1] = [file,ls]  # adding a row
                df.index = df.index + 1  # shifting index
                df = df.sort_index()  # sorting by index
            except:
                df.to_parquet(str(project_path) + "/Text_col.parquet")
                
        df.to_parquet(str(project_path) + "/Text_col.parquet")
        os.chdir(project_path)
        

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
        print(str(i) + " Files removed")
    
    # Process
    os.chdir(project_path)
    print('Preparing file structre...')
    try:
        os.mkdir(pdfs_path)
        os.mkdir(txt_path)
    except:
        print('Structure already exists!')

    print('Reading XLS file...')
    data = pd.read_csv(overton_table_path)

    print("Downloading pdfs...")
    missing = download(pdfs_path,data)
    print(str(missing) +' files are missing')

    print('Removing corrupted files')
    remove_corrupted_files(pdfs_path)

    print("Converting to raw paragraphs...")
    names = ['paragraphs_for_pdfs','pdf_internal_name']
    convert_pdf_folder_to_paragraphs(pdfs_path,project_path,names)

    def custom_regex(text):
        url = re.compile(r'(([a-z]{3,6}://)|(^|\s))([a-zA-Z0-9\-]+\.)+[a-z]{2,13}[\.\?\=\&\%\/\w\-]*\b([^@]|$)')
        whitespaces = re.compile(r'\n|\t')
        num = re.compile(r'[0-9]{1,30}')
        parenth = re.compile(r'\([0-9]{1,4}\)')
        email = re.compile(r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)')
        multiple_spaces = re.compile(r' {2,40}')
        html=re.compile(r'<.*?>')
        table=str.maketrans('','',string.punctuation)
        marks = re.compile(r'•')

        text = whitespaces.sub(r' ',text)
        text = url.sub(r' ',text)
        text = email.sub(r' ',text)
        text = html.sub(r' ',text)
        text = parenth.sub(r' ',text)
        text = text.translate(table)
        text = num.sub(r' ',text)
        text = marks.sub(r' ',text)
        text = multiple_spaces.sub(r' ',text)
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
        ls = copy.deepcopy(df['Text'])
        for i in range(len(ls)): #document
            for j in range(len(ls[i])-1):  # paragraph
                if(nwords(ls[i][j]) < treshold):
                    ls[i][j+1] = ls[i][j] + ' '+ ls[i][j+1]
                    ls[i][j] =''
            ls[i] = [s for s in ls[i] if s!='']
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
        return n_pargraphs,n_words
    
    def merge_tables(meta,subtable):
        huge_table = copy.deepcopy(meta)
        huge_table['Name'] = [None]*len(huge_table)
        huge_table['Text'] = [None]*len(huge_table)
        huge_table['n_paragraphs'] = [None]*len(huge_table)
        huge_table['n_words'] = [None]*len(huge_table)

        for i in range(len(subtable)):
            idx = int(subtable['Name'][i].strip('.pdf'))
            huge_table['Name'][idx] = subtable['Name'][i]
            huge_table['Text'][idx] = subtable['Text'][i]
            huge_table['n_paragraphs'][idx] = subtable['n_paragraphs'][i]
            huge_table['n_words'][idx] = subtable['n_words'][i]

        return huge_table
    
    print("Loading paragraphs files...")
    paragraph_df = pd.read_parquet(str(project_path) + "/Text_col.parquet")

    print("Merging paragraphs...")
    merged_paragraphs = paragraph_management(paragraph_df)

    print("Cleaning paragraphs...")
    clean_paragraphs = clean(merged_paragraphs)

    print("Preparing statistics...")
    np,nw = prepare_stats(clean_paragraphs)

    print("Creating subtable...")
    subtable = pd.DataFrame(list(zip(paragraph_df['Name'],clean_paragraphs,np,nw)),columns = ['Name','Text','n_paragraphs','n_words'])

    print('Reading XLS file...')
    data = pd.read_csv(overton_table_path)

    print('Preparing final table...')
    final_table = merge_tables(data,subtable)

    print('Saving final table')
    final_table.to_parquet(str(project_path) + "/Final_table.parquet", index=False)

if __name__=="__main__":
    typer.run(main)
