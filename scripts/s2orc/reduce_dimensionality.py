import pandas as pd
import typer
import os
import joblib

EMBEDDINGS_DIR_PATH = "data/s2orc/embeddings/"
import numpy as np

def main(file_name:str,n_components:int, gpu:bool=False):
    print("Reducing dimensionality to {} components".format(n_components))
    print("Loading libraries...")
    if gpu:
        print("Using GPU")
        import cuml
        reducer = cuml.UMAP(n_components=n_components, random_state=42)
        joblib.dump(reducer, os.path.join(EMBEDDINGS_DIR_PATH, 'umap_reducers', file_name))
    else:
        print("Using CPU")
        import umap
        reducer = umap.UMAP(n_components=n_components, random_state=42)
    
    in_file_path = os.path.join(EMBEDDINGS_DIR_PATH, file_name)
    out_file_name = f"reduced_{str(n_components)}_{file_name}"
    out_file_path = os.path.join(EMBEDDINGS_DIR_PATH, out_file_name)
    print("Loading from:",in_file_path)
    print("Will save to:",out_file_path)
    print("Loading...")
    df = pd.read_parquet(in_file_path)

    emb = df['embedding']

    print("Reducing...")
    reduced_embeddings = reducer.fit_transform(np.stack(emb))

    df['embedding'] = list(reduced_embeddings)
    print("Saving...")
    df.to_parquet(out_file_path)

    print("Done!")
if __name__=="__main__":
    typer.run(main)
