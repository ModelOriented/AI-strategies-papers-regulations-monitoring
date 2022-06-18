import pandas as pd
import numpy as np
import typer
from collections import Counter


def ctidf_names(memes_mappings:pd.DataFrame,file_name:str):
    print(file_name)
    def split_function(s:str):
        return s.split()

    def max_dict(d:dict):
        if len(d)!=0:
            return max(d, key=d.get)
        else:
            return ''

    b = memes_mappings['chunk'].explode().str.split(' ').explode()
    all_words_counter = Counter(b)

    clusters = (memes_mappings.groupby(['meme_id'])
        .agg({'chunk': lambda x: x.tolist()})
        .rename({'chunk' : 'cluster'},axis=1))

    #calculate mean number of words per cluster:
    A = b.size/clusters.size

    clusters['cluster'] = clusters['cluster'].apply(' '.join).apply(split_function)

    #length of each cluster:
    clusters['length'] = [len(x) for x in clusters['cluster']]

    #counter of words in cluster
    clusters['cluster_counter'] = clusters['cluster'].apply(Counter)

    #calculate ctdf for each word in cluster:
    list_of_dicts = []
    for _,row in clusters.iterrows():
        name_dict = {}
        for key in row['cluster_counter']:
            name_dict[key] = row['cluster_counter'][key]*np.log(1+A/all_words_counter[key])
        list_of_dicts.append(name_dict)
    clusters['ctdf'] = list_of_dicts
    #return series of names of clusters
    clusters['cluster_name'] = clusters['ctdf'].apply(max_dict)
    print(clusters.head())
    clusters.to_parquet('data/s2orc/clusterings/clusters_names/'+file_name)

def main(path:str):
    df= pd.read_parquet(path)
    file_name = path.split("/")[-1]
    return ctidf_names(df, file_name)

if __name__ == "__main__":
    typer.run(main)

