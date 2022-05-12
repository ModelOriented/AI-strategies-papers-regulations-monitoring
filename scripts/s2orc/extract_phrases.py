import pandas as pd
import joblib
from tqdm import tqdm
import textacy.extract
import typer

tqdm.pandas()

def extract_noun_chunks(doc):
    try:
        return [
            chunk.text
            for chunk in textacy.extract.basics.noun_chunks(doc)
            if not len(chunk) == 1 or chunk[0].pos_ in {"PROPN", "NOUN"}
        ]
    except AttributeError:
        return []


def main(in_path: str, out_path: str):
    print("Loading data...")
    df = joblib.load(in_path)

    print("Processing data...")
    df["noun_chunks"] = df["doc"].progress_apply(extract_noun_chunks)

    del df["doc"]
    print("Saving data...")
    df.to_parquet(out_path, index=False)


if __name__=='__main__':
    typer.run(main)