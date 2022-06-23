import pandas as pd
import numpy as np
import typer
from collections import Counter
import os.path

IN_DIR = 'data/s2orc/chunk_meme_mappings'
OUT_DIR = 'data/s2orc/clusterings/clusters_names/'

def ctidf_names(memes_mappings: pd.DataFrame, chunks: pd.Series) -> pd.DataFrame:
    chunks_counter = Counter(chunks.explode())

    def split_function(s: str):
        return s.split()

    def max_dict(d: dict):
        if len(d) != 0:
            return max(d, key=d.get)
        else:
            return ''

    b = memes_mappings['chunk'].str.split(' ').explode() # flatten to all words
    all_words_counter = Counter(b)

    clusters = (memes_mappings.groupby(['meme_id']).agg({
        'chunk':
        lambda x: x.tolist()
    }).rename({'chunk': 'cluster'}, axis=1)) # creates mapping of clusters to unique noun chunks list

    #calculate mean number of words per cluster:
    A = b.size / clusters.size

    clusters['cluster'] = clusters['cluster'].apply(
        ' '.join).apply(split_function)

    #length of each cluster:
    clusters['length'] = [len(x) for x in clusters['cluster']]

    #counter of words in cluster
    clusters['cluster_counter'] = clusters['cluster'].apply(Counter)

    #calculate ctdf for each word in cluster:
    list_of_dicts = []
    for _, row in clusters.iterrows():
        words_to_score = {}
        for key in row['cluster_counter']:
            words_to_score[key] = row['cluster_counter'][key] * np.log(
                1 + A / all_words_counter[key])
        list_of_dicts.append(words_to_score)
    clusters['ctdf'] = list_of_dicts
    #return series of names of clusters
    clusters['cluster_name'] = clusters['ctdf'].apply(max_dict)
    print(clusters.head())


def main(file_name: str):
    in_path = os.path.join(IN_DIR, file_name)
    out_path = os.path.join(OUT_DIR, file_name)
    df = pd.read_parquet(in_path)
    file_name = file_name.split("/")[-1]
    df_out = ctidf_names(df)

    df_out.to_parquet(out_path) 


if __name__ == "__main__":
    typer.run(main)
