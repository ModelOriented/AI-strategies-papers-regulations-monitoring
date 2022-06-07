import pandas as pd
import umap
import typer


import numpy as np

def main(n_components:int, gpu:bool=False):
    print("Reducing dimensionality to {} components".format(n_components))
    print("Loading data...")
    if gpu:
        print("Using GPU")
        import cuml
        reducer = cuml.UMAP(n_components=n_components, random_state=42)
    else:
        print("Using CPU")
        reducer = umap.UMAP(n_components=n_components, random_state=42)

    df = pd.read_parquet('data/s2orc/embeddings/big_cleaned_mini_all-MiniLM-L6-v2.parquet')
    out = f'data/s2orc/embeddings/reduced_{n_components}_big_cleaned_mini_all-MiniLM-L6-v2.parquet'

    emb = df['embedding']

    print("Reducing...")
    reduced_embeddings = reducer.fit_transform(list(emb))

    df['embedding_reduced'] = list(reduced_embeddings)
    del df['embedding']
    print("Saving...")
    df.to_parquet(out)


if __name__=="__main__":
    typer.run(main)