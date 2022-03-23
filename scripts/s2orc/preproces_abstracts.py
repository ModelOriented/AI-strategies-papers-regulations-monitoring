import pandas as pd
import joblib

from tqdm import tqdm
import spacy
en = spacy.load('en_core_web_sm')
tqdm.pandas()


df = pd.read_csv("data/s2orc/s2orc_citations_filtered_with_mag_id.csv", index_col=0)
df = df[~df.abstract.isna()]

df['doc'] =df.abstract.progress_apply(en)
df['lemmas'] = df['doc'].apply(lambda doc: [t.lemma_ for t in doc if t.is_alpha if not t.is_stop if not t.is_punct])

joblib.dump(df, "data/s2orc/s2orc_citations_filtered_with_mag_id_with_spacy.pkl")
