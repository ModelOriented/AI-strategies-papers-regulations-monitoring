from mars.utils import set_root_path
set_root_path()
from tqdm.notebook import tqdm 

from collections import Counter

import json

import pandas as pd
from sentence_transformers import SentenceTransformer

import typer



def main(input_path:str, output_path: str, sentences_embedding:str = 'all-MiniLM-L6-v2'):
    print("Loading data...")
    df = pd.read_parquet(input_path)
    chunks_count = Counter([chunk for chunks in df['noun_chunks'] for chunk in chunks])
    all_chunks = [chunk for chunk, count in chunks_count.items()]

    print(f"There are {len(all_chunks)} unique noun chunks.")

    print(f"Loading model {sentences_embedding}...")
    model = SentenceTransformer(sentences_embedding)
    def embedd(text):
        return model.encode(text)

    print("Processing data...")

    chunk_to_embedding = {chunk: embedd(chunk) for chunk in tqdm(all_chunks)}

    with open(output_path, 'w') as f:
        json.dump(chunk_to_embedding, f)


if __name__=='__main__':
    typer.run(main)