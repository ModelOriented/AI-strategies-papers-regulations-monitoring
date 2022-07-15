
import embedd_noun_chunks
import cluster
import reduce_dimensionality
import create_meme_names
from meme_score import meme_score
import prepare_memes
import memes_flows

import numpy as np
import pandas as pd

import typer


def main(in_path:str,out_path_embedding:str,out_path_cluster:str, out_path_meme_score:str, sentences_embedding: str = "all-MiniLM-L6-v2", batch_size_embedd:int=32, multiprocess_embedd:bool=False, do_reduce:bool - True, n_components_reduce:int=300, gpu_reduce:bool=False,
n_jobs_cluster: int = -1, cluster_selection_epsilons: str = ".0", gpu_cluster: bool = False, min_clust_size: int = 5, metric_cluster: str = 'euclidean',conditioning_meme_score:str='is_big_tech'):
    #embedd noun_chunks
    chunk_to_embedding = embedd_noun_chunks(in_path, sentences_embedding,batch_size_embedd,multiprocess_embedd)#return chunk to embeddinng
    
    #reduce dimensionality
    if do_reduce:
        reduced = reduce_dimensionality(chunk_to_embedding, n_components_reduce, gpu_reduce)

    pd.DataFrame(chunk_to_embedding).to_parquet(out_path_embedding)#saving

    df_cluster = cluster(reduced,
         n_jobs_cluster,
         cluster_selection_epsilons,
         gpu_cluster,
         min_clust_size,
         metric_cluster)

    pd.DataFrame(df_cluster).to_parquet(out_path_cluster)#saving

    #TODO: the file processed_big and processed_big_with_affiliations is read inside the scripts. Do we want to specify paths in clearer way?
    df_cluster, chunk_to_meme = prepare_memes(df_cluster) #skąd plik processed_big? z notebooka

    meme_to_name = create_meme_names(chunk_to_meme)

    df_meme_score = meme_score(df_cluster, conditioning_meme_score)

    df_meme_score['meme_name'] = df_meme_score.apply(lambda meme: meme_to_name.iloc[meme['meme_id']])

    pd.DataFrame(df_meme_score).to_parquet(out_path_meme_score)
    #apka do streamlita wczytuje noun chunki


if __name__ == "__main__":
    typer.run(main)