
import embedd_noun_chunks
import cluster
import reduce_dimensionality
import create_meme_names
from meme_score import meme_score
import prepare_memes
import memes_flows

import typer
import numpy as np
import pandas as pd


def main(out_path_embedding:str,out_path_cluster:str, out_path_meme_score:str, in_path:str='data/s2orc/processed_big.parquet',cit_path = r'C:/Users/ppaul/Documents/AI-strategies-papers-regulations-monitoring/data/s2orc/big_ai_dataset.parquet', sentences_embedding: str = "all-MiniLM-L6-v2", batch_size_embedd:int=32, multiprocess_embedd:bool=False, do_reduce:bool = True, n_components_reduce:int=300, gpu_reduce:bool=False,
n_jobs_cluster: int = -1, cluster_selection_epsilons: str = ".0", gpu_cluster: bool = False, min_clust_size: int = 5, metric_cluster: str = 'euclidean',conditioning_meme_score:str='is_big_tech'):
    #embedd noun_chunks
    print('EMBEDDING NOUN CHUNKS')
    chunk_to_embedding = embedd_noun_chunks.main(in_path, sentences_embedding,batch_size_embedd,multiprocess_embedd)#return chunk to embeddinng
    
    #reduce dimensionality
    if do_reduce:
        print('REDUCING')
        reduced = reduce_dimensionality.main(chunk_to_embedding, n_components_reduce, gpu_reduce)

    pd.DataFrame(chunk_to_embedding).to_parquet(out_path_embedding)#saving

    print('CLUSTERING')
    df_cluster = cluster.main(reduced,
         n_jobs_cluster,
         cluster_selection_epsilons,
         gpu_cluster,
         min_clust_size,
         metric_cluster)


    pd.DataFrame(df_cluster).to_parquet(out_path_cluster)#saving

    print('PREPARING MEMES')
    df_cluster, chunk_to_meme = prepare_memes.main(df_cluster,in_path,cit_path) #na razie nie ma informacji o cytowaniach
    print('CREATING NAMES')
    meme_to_name = create_meme_names.main(chunk_to_meme,in_path)
    print('CALCULATING MEME SCORE')
    df_meme_score = meme_score(df_cluster, conditioning_meme_score)

    df_meme_score['meme_name'] = df_meme_score['meme_id'].map(meme_to_name["best_tfidf"])

    pd.DataFrame(df_meme_score).to_parquet(out_path_meme_score)
    #apka do streamlita wczytuje noun chunki


if __name__ == "__main__":
    typer.run(main)
    #main('data/s2orc/'+"all-MiniLM-L6-v2"+'.parquet','data/s2orc/clusters.parquet','data/s2orc/final.parquet',in_path = 'data/s2orc/processed_big_test.parquet')
    #typer.run(main)