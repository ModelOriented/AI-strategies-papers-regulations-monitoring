import copy
import re
import string

import joblib
import pandas as pd
import typer


def main(out_path : str,pdfs_in_paragraphs_path : str,pdfs_internal_names_path : str,overton_table_path: str):
    """
    Expands original Oveton dataframe by adding Text in paragraphs and some stats

    out_path                    : output path for final table
    pdfs_in_paragraphs_path     : path to the file with paragraph per document list of lists
    pdfs_internal_names_path    : path to the file with document internal names, that correspond to pdfs_in_paragraphs_path
    overton_table_path          : path to the original Overton table
    """

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
    pdfs_in_pargraphs = joblib.load(pdfs_in_paragraphs_path)
    pdfs_internal_names = joblib.load(pdfs_internal_names_path)

    print("Merging paragraphs...")
    merged_paragraphs = paragraph_management(pdfs_in_pargraphs)

    print("Cleaning paragraphs...")
    clean_paragraphs = clean(merged_paragraphs)

    print("Preparing statistics...")
    np,nw = prepare_stats(clean_paragraphs)

    print("Creating subtable...")
    subtable = pd.DataFrame(list(zip(pdfs_internal_names,clean_paragraphs,np,nw)),columns = ['Name','Text','n_paragraphs','n_words'])

    print('Reading XLS file...')
    data = pd.read_excel(overton_table_path)

    print('Preparing final table...')
    final_table = merge_tables(data,subtable)

    print('Saving final table')
    final_table.to_parquet(out_path, index=False)


if __name__=="__main__":
    typer.run(main)
