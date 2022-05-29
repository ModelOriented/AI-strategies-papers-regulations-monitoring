from tqdm.notebook import tqdm

import orjson as json

import pandas as pd
from sentence_transformers import SentenceTransformer

import typer
import os

def main(
    input_path: str, output_path: str, sentences_embedding: str = "all-MiniLM-L6-v2", batch_size:int=32, multiprocess:bool=False):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    print("Loading data...")
    df = pd.read_parquet(input_path)
    print("Loaded data:", len(df))
    all_chunks = list(set([chunk.lower() for chunks in df["noun_chunks"] for chunk in chunks]))

    print(f"There are {len(all_chunks)} unique noun chunks.")

    print(f"Loading model {sentences_embedding}...")
    model = SentenceTransformer(sentences_embedding)
    

    def embedd(texts):
        if multiprocess:
            pool = model.start_multi_process_pool()
            return model.encode_multi_process(texts, pool=pool, batch_size=batch_size)
        else:
            return model.encode(texts, batch_size=batch_size, show_progress_bar=True, convert_to_numpy=False)

    print("Processing data...")
    embeddings = embedd(all_chunks)
    chunk_to_embedding = [(chunk, list(embeddings[i].astype(float))) for i, chunk in enumerate(all_chunks)]
    
    pd.DataFrame(chunk_to_embedding, columns=["chunk", "embedding"]).to_parquet(output_path)


if __name__ == "__main__":
    typer.run(main)
