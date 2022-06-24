import joblib
import pandas as pd
import os
from sentence_transformers import SentenceTransformer
import cuml
import numpy as np
import scipy
import typer


def main(sentences_embedding:str, multiprocess:bool, batch_size:int):
    ai_act_chunks = pd.read_csv('data/s2orc/chunks_from_ai_act1.csv')
    all_chunks = list(set(ai_act_chunks['chunk']))

    print('Embedding ...')
    model = SentenceTransformer(sentences_embedding)

    def embedd(texts):
        if multiprocess:
            pool = model.start_multi_process_pool()
            return model.encode_multi_process(texts, pool=pool, batch_size=batch_size)
        else:
            return model.encode(texts, batch_size=batch_size, show_progress_bar=True, convert_to_numpy=False)

    embeddings = embedd(all_chunks)
    chunk_to_embedding = [(chunk, list(embeddings[i].tolist())) for i, chunk in enumerate(all_chunks)]

    df = pd.DataFrame(chunk_to_embedding, columns=["chunk", "embedding"])

    print('UMAP ...')

    reducer = joblib.load('data/s2orc/embeddings/umap_reducers/big_cleaned_phrase-bert.parquet')
    reduced_embeddings = reducer.transform(np.stack(embeddings))

    df['reduced_embedding'] = list(reduced_embeddings)

    print('Finding nearest points ...')
    base_embeddings = pd.read_parquet('data/s2orc/embeddings/reduced_300_big_cleaned_phrase-bert.parquet')

    Tree = scipy.spatial.cKDTree(base_embeddings['embedding'], leafsize=100)
    closest_idx = []
    for item in list(df['reduced_embedding']):
        TheResult = Tree.query(item, k=1, distance_upper_bound=6)
        closest_idx.append(TheResult.i)
    df['closest_idx'] = closest_idx

    print('Looking for closest clusters ...')

    clusts = pd.read_parquet('data/s2orc/clusterings/reduced_300_big_cleaned_phrase-bert_eps_0.2_min_clust_size_3.parquet', columns=['meme_id'])
    clusts.reset_index(inplace=True)
    df = df.merge(clusts, left_on='closest_idx', right_on='index')
    df.to_parquet('data/s2orc/clusterings/ai_act/reduced_300_big_cleaned_phrase-bert_eps_0.2_min_clust_size_3.parquet')

if __name__ == '__main__':
    typer.run(main)
