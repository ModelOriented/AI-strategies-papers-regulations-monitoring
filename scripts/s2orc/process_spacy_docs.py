import pandas as pd
import spacy
import joblib
from tqdm import tqdm 

tqdm.pandas()
import mars.s2orc.loading

OUT_PATH = 'data/s2orc/s2orc_ai_prefiltered_processed_with_doi.pkl'

df = mars.s2orc.loading.load_s2orc_prefiltered()
df2 = df[~df['doi'].isnull()]
en = spacy.load('en_core_web_md')

def process(text):
    try:
        return en(text)
    except Exception:
        print("Error: {}".format(text))
        return ""

df2['doc'] = df2['abstract'].progress_apply(process)

joblib.dump(df2, OUT_PATH)
