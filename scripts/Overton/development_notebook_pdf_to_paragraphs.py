# %% 
# Imports
import copy
import os
import re
import string

import joblib
import pandas as pd
import PyPDF2
import requests
from matplotlib.pyplot import close
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBoxHorizontal
from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.pdfpage import PDFPage

# %% 
# Read Fle
data = pd.read_excel('results-2022-06-30.xlsx')
data[:3]
meta = pd.DataFrame(data)
meta
#%% Download single file
#url = df['PDF URL'][0]
#r = requests.get(url, allow_redirects=True)
#st = 'pdfs/' + df['Title'][0] + '.pdf'
#str = 'pdfs/' +  + '.pdf'
#open(st, 'wb').write(r.content)
#%% 
# Download all files
missing = 0
k = 0
for i in range(k, len(data)):
    try:
        url = data['PDF URL'][i]
        r = requests.get(url, allow_redirects=True)
        #str = 'pdfs/' + df['Title'][i] + '.pdf'
        st = 'pdfs/' + str(i) + '.pdf'
        open(st, 'wb').write(r.content)
    except:
        url = data['PDF URL'][i]
        if(url == 'nan'): missing += 1



# %%
# Functions converting pdfs to paragraphs
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
        #one_list = one_list + ls
        #one_list_lens.append(len(ls))
    os.chdir(initial_path)
    #w_path = "/content/drive/MyDrive/Commission Adoption Feedback/"
    joblib.dump(list_of_lists,w_path + names[0])
    #joblib.dump(one_list,w_path + names[1])
    joblib.dump(names,w_path + names[2])
    #joblib.dump(one_list_lens,w_path + names[3])

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
# Remove corrupted files
remove_corrupted_files('small_pdfs')
remove_corrupted_files('pdfs')

# %% 
# Prepare documents paragraphs
convert_pdf_folder_to_paragraphs('C:/Users/Hubert/Documents/DarlingProject/Overton/small_pdfs','C:/Users/Hubert/Documents/DarlingProject/Overton', 'C:/Users/Hubert/Documents/DarlingProject/Overton/small_txts/', ['a','b','c','d'])

# %%
# Load prepared paragraph info for documents
df = joblib.load('small_txts0.pdf')
#df2 = joblib.load('small_txts1.pdf')
df3 = joblib.load('small_txts10.pdf')
#df4 = joblib.load('small_txts11.pdf')



#%%
# Text cleaning and pargraph merging functions
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
# Glue the paragraphs together and clean them alter on
new_df = paragraph_management(df)
clean_df = clean(new_df)


# %%
# Prepare stats for files
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


#%%
np,nw = prepare_stats(clean_df)
# %%
# Create table with text, title and so on
subtable = pd.DataFrame(list(zip(df3,clean_df,np,nw)),columns = ['Name','Text','n_paragraphs','n_words'])
subtable
# %%
# Merge tables
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
# %%
