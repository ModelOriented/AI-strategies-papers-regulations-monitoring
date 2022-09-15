
import embedd_noun_chunks
import cluster
import os
import reduce_dimensionality
import create_meme_names
from meme_score import meme_score
import prepare_memes
import affiliation_pipeline

import typer
import numpy as np
import pandas as pd
from typing import List

def main(out_path_embedding: str,
out_path_cluster: str, 
out_path_meme_score: str, 
in_path: str = 'data/s2orc/processed_big.parquet',
cit_path: str = 'data/s2orc/big_ai_dataset.parquet', 
sentences_embedding: str = "all-MiniLM-L6-v2", 
json_input = 'data/s2orc/doi_to_authorship_big.json',
aff_output_path = 'data/s2orc/big_ai_dataset_with_affiliations.parquet',
batch_size_embedd: int = 32, 
multiprocess_embedd: bool = False, 
do_reduce: bool = True, 
n_components_reduce: int = 300, 
gpu_reduce: bool = typer.Option(False),
n_jobs_cluster: int = -1, 
cluster_selection_epsilons: str = ".0", 
gpu_cluster: bool = False, 
min_clust_size: int = 5, 
metric_cluster: str = 'euclidean',
condition_list: List[str] = ['PL'],
category: str = 'country',
do_memes: bool = typer.Option(True)
):
    #embedd noun_chunks
    if not os.path.exists(out_path_cluster):
        print('EMBEDDING NOUN CHUNKS')
        chunk_to_embedding = embedd_noun_chunks.embedd_noun_chunks(in_path, sentences_embedding,batch_size_embedd,multiprocess_embedd)#return chunk to embeddinng
        
        #reduce dimensionality
        if do_reduce:
            print('REDUCING')
            reduced = reduce_dimensionality.reducing(chunk_to_embedding, n_components_reduce, gpu_reduce)

        pd.DataFrame(chunk_to_embedding).to_parquet(out_path_embedding)#saving

        print('CLUSTERING')
        df_cluster = cluster.clustering(reduced,
            n_jobs_cluster,
            cluster_selection_epsilons,
            gpu_cluster,
            min_clust_size,
            metric_cluster)

        pd.DataFrame(df_cluster).to_parquet(out_path_cluster)#saving
    else:
        print('READING IN CLUSTERS')
        df_cluster = pd.read_parquet(out_path_cluster)
        df_cluster.index.name = None
    if do_memes:
        if not os.path.exists(aff_output_path):
            print('GETTING AFFILIATIONS')
            df_af = affiliation_pipeline.affiliations(condition_list,category,cit_path,json_input,aff_output_path)
        else:
            print('READING IN AFFILIATIONS')
            df_af = pd.read_parquet(aff_output_path)

        print('PREPARING MEMES')
        df_cluster, chunk_to_meme = prepare_memes.preparing(df_cluster, df_af)

        print('CREATING NAMES')
        meme_to_name = create_meme_names.names(chunk_to_meme,df_cluster)

        print('CALCULATING MEME SCORE')
        df_meme_score = meme_score(df_cluster)

        df_meme_score['meme_name'] = df_meme_score['meme_id'].map(meme_to_name["best_tfidf"])

        pd.DataFrame(df_meme_score).to_parquet(out_path_meme_score)


if __name__ == "__main__":
    typer.run(main)
    #main('data/s2orc/'+"all-MiniLM-L6-v2"+'.parquet',"C:/Users/ppaul/Documents/AI-strategies-papers-regulations-monitoring/data/s2orc/results/reduced_300_big_cleaned_mini_all-MiniLM-L6-v2_eps_0.0.parquet",'data/s2orc/final.parquet')

    