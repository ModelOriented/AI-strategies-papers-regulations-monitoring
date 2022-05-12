import pandas as pd
import joblib
from tqdm import tqdm
import sklearn.cluster
import typer
from collections import Counter
import json
tqdm.pandas()


def main(in_table_path: str, in_json_path: str, out_path: str):
    print("Loading data...")
    df = pd.read_parquet(in_table_path)
    print(len(df))


    chunk_to_embedding = json.load(open(in_json_path))
    print(len(chunk_to_embedding))
    model = sklearn.cluster.DBSCAN()
    print("Clutering chunks...")
    clusters = model.fit_predict(list(chunk_to_embedding.values()))
    print("Saving data...")
    df_out = pd.DataFrame([{"cluster": cluster, "chunk":chunk, "embedding":embedding} for cluster, (chunk, embedding) in zip(clusters, chunk_to_embedding.items())])
    df_out.to_parquet(out_path)


if __name__=='__main__':
    typer.run(main)