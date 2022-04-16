import pandas as pd
import spacy
import joblib
from tqdm import tqdm
import textacy.extract
import textacy.extract.keyterms

tqdm.pandas()

IN_PATH = "data/s2orc/s2orc_ai_prefiltered_processed_with_doi.pkl"
OUT_PATH = "data/s2orc/extracted.csv"


def extract_noun_chunks(doc):
    try:
        return [
            chunk.text
            for chunk in textacy.extract.basics.noun_chunks(doc)
            if not len(chunk) == 1 or chunk[0].pos_ in {"PROPN", "NOUN"}
        ]
    except AttributeError:
        return []


df = joblib.load(IN_PATH)
en = spacy.load("en_core_web_md")


df["noun_chunks"] = df["doc"].progress_apply(extract_noun_chunks)
df["keywords_text_rank"] = df["doc"].progress_apply(
    textacy.extract.keyterms.textrank
)

del df["doc"]

df.to_csv(OUT_PATH, index=False)
