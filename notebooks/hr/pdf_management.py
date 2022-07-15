# %% imports
from copy import deepcopy
from matplotlib.pyplot import close
import pandas as pd
import requests
from tqdm import tqdm 
import pdfminer
import os
import PyPDF2
import joblib

from pdfminer.converter import TextConverter, PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBoxHorizontal
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser

tqdm.pandas()
# %%
df = pd.read_excel('results-2022-06-30.xlsx')
df[:3]

#%% Download single file
#url = df['PDF URL'][0]
#r = requests.get(url, allow_redirects=True)
#st = 'pdfs/' + df['Title'][0] + '.pdf'
#str = 'pdfs/' +  + '.pdf'
#open(st, 'wb').write(r.content)
#%% Download all files
missing = 0
k = 0
for i in range(k, len(df)):
    try:
        url = df['PDF URL'][i]
        r = requests.get(url, allow_redirects=True)
        #str = 'pdfs/' + df['Title'][i] + '.pdf'
        st = 'pdfs/' + str(i) + '.pdf'
        open(st, 'wb').write(r.content)
    except:
        url = df['PDF URL'][i]
        if(url == 'nan'): missing += 1
# %%
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

# %%
paragraphs = convert_pdf_to_paragraphs('pdfs/228.pdf')
paragraphs

# %%
def convert_pdf_folder_to_paragraphs(path,initial_path,w_path,names):
    """
    Desc:   Converts all pdf files in the given path to paragraphs and then saves them as 4 pickles of lists.
    Input:  path to the folder contatining pdfs
            initial_path to the folder of project
            w_path to the folder wehere txt files are saved
            names list of string with names of saved pickles
    Output: pickled list of lists with paragraphs per document
            pickled list of all paragraphs
            pickled list of doc names
            pickled list of doc lengths (in paragraphs)
    """
    list_of_lists = []
    one_list = []
    names = []
    one_list_lens = []
    #path = "/content/drive/MyDrive/Commission Adoption Feedback/Pdf"
    #initial_path = "/content"
    os.chdir(path)

    for file in os.listdir():
        if file.endswith(".pdf"):
            file_path = f"{path}/{file}"
        print(file_path)
        names.append(file)
        os.chdir(initial_path)
        ls = convert_pdf_to_paragraphs(file_path)
        list_of_lists.append(ls)
        one_list = one_list + ls
        one_list_lens.append(len(ls))
    os.chdir(initial_path)
    #w_path = "/content/drive/MyDrive/Commission Adoption Feedback/"
    joblib.dump(list_of_lists,w_path + names[0])
    joblib.dump(one_list,w_path + names[1])
    joblib.dump(names,w_path + names[2])
    joblib.dump(one_list_lens,w_path + names[3])


# %% Czyszczenie z zepsutych pdfów
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
# %%
remove_corrupted_files('small_pdfs')

# %%
remove_corrupted_files('pdfs')
# %%
convert_pdf_folder_to_paragraphs('C:/Users/Hubert/Documents/DarlingProject/Overton/small_pdfs','C:/Users/Hubert/Documents/DarlingProject/Overton', 'C:/Users/Hubert/Documents/DarlingProject/Overton/small_txts/', ['a','b','c','d'])
# %%
df = joblib.load('small_txts0.pdf')
df[0]
# %%
df2 = joblib.load('small_txts1.pdf')
df2
# %%
df3 = joblib.load('small_txts10.pdf')
df3
# %%
df4 = joblib.load('small_txts11.pdf')
df4
#%%
import re
import string
import copy
#%%
def custom_regex(text):
    whitespaces = re.compile(r'\n')
    bignum = re.compile(r'[0-9]{5,30}')
    parenth = re.compile(r'\([0-9]{1,4}\)')
    email = re.compile(r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)')
    multiple_spaces = re.compile(r' {2,40')
    html=re.compile(r'<.*?>')
    table=str.maketrans('','',string.punctuation)

    text = whitespaces.sub(r'',text)
    text = bignum.sub(r'',text)
    text = parenth.sub(r'',text)
    text = email.sub(r'',text)
    text = multiple_spaces.sub(r'',text)
    text = html.sub(r'',text)
    text = text.translate(table)
    return text
#%%
def clean(lst):
    ls = copy.deepcopy(lst)
    for i in range(len(ls)):
        for j in range(len(ls[i])):
            ls[i][j] = custom_regex(ls[i][j])
    return ls
#%%
def nwords(string):
    return len(string.split())
#%%
def paragraph_management(lst, treshold=60):
    ls = copy.deepcopy(lst)
    for i in range(len(ls)): #document
        for j in range(len(ls[i])-1):  # paragraph
            if(nwords(ls[i][j]) < treshold):
                ls[i][j+1] = ls[i][j] + ' '+ ls[i][j+1]
                ls[i][j] =''
        ls[i] = [s for s in ls[i] if s!='']
    return ls
# %%
new_df = paragraph_management(df)

# %%
clean_df = clean(new_df)
clean_df# %%

# %%
copy.deepcopy