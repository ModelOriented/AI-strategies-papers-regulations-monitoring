from tqdm.notebook import tqdm

import json

import pandas as pd
from sentence_transformers import SentenceTransformer

import typer
import os

def main(
    input_path: str, output_path: str, sentences_embedding: str = "all-MiniLM-L6-v2"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    print("Loading data...")
    df = pd.read_parquet(input_path)
    print("Loaded data:", len(df))
    all_chunks = list(set([chunk.lower() for chunks in df["noun_chunks"] for chunk in chunks]))

    print(f"There are {len(all_chunks)} unique noun chunks.")

    print(f"Loading model {sentences_embedding}...")
    model = SentenceTransformer(sentences_embedding)

    def embedd(texts):
        return model.encode(texts)

    print("Processing data...")
    embeddings = embedd(all_chunks)
    chunk_to_embedding = {chunk: list(embeddings[i]) for i, chunk in tqdm(enumerate(all_chunks))}

    with open(output_path, "w") as f:
        json.dump(chunk_to_embedding, f)


if __name__ == "__main__":
    typer.run(main)
