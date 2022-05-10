import pandas as pd
import spacy
import joblib
from tqdm import tqdm 
import typer

tqdm.pandas()


def main(in_path: str, out_path:str, spacy_model_name: str='en_core_web_md'):
    print("Loading data...")
    df = pd.read_parquet(in_path)
    print("Loading spacy model...")
    en = spacy.load(spacy_model_name)
    def process(text):
        try:
            return en(text)
        except Exception:
            print("Error: {}".format(text))
            return ""

    df['doc'] = df['abstract'].progress_apply(process)
    print("Saving...")
    joblib.dump(df, out_path)


if __name__=="__main__":
    typer.run(main)