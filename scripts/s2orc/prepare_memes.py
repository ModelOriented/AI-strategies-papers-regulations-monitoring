#from mars.utils import set_root_path
import pandas as pd

import os
from collections import Counter
import typer
from tqdm import tqdm

N_TO_REMOVE = 10

def make_chunk_to_meme_id(df_clusters) -> dict:
    """Map all chunks to their meme id, dependent on cluster. Outliers are given new ids."""
    next_id = max(df_clusters['cluster']) + 1
    chunk_to_meme_id = dict()
    for _, r in df_clusters.iterrows():
        id = r['cluster']
        if id == -1:
            id = next_id
            next_id += 1
        chunk_to_meme_id[r['chunk']] = id
    return chunk_to_meme_id


def merge_noun_chunks_with_affiliations(df_noun_chunks,df_affiliations):

    memes_df = pd.merge(df_affiliations.drop("noun_chunks_cleaned", axis=1),
                        df_noun_chunks[['paper_id', 'noun_chunks_cleaned']],
                        on='paper_id',
                        how='left')
    #df2 = df2[df2['institutions'].str.len()!=0]
    memes_df['inbound_citations'] = memes_df['inbound_citations'].apply(
        lambda ids: [int(id) for id in ids])
    memes_df['outbound_citations'] = memes_df['outbound_citations'].apply(
        lambda ids: [int(id) for id in ids])

    memes_df = memes_df[~memes_df['noun_chunks_cleaned'].isna()] 

    return memes_df


def get_meme_statiscics(df_memes, chunk_to_meme):
    # creating memes stats
    all_memes = df_memes['memes'].explode()
    all_inbound_memes = df_memes['inbound_memes'].explode()
    all_outbount_memes = df_memes['outbound_memes'].explode()
    memes_count = Counter(all_memes)
    inbound_memes_count = Counter(all_inbound_memes)
    outbound_memes_count = Counter(all_outbount_memes)

    meme_to_cluster_size = chunk_to_meme.groupby('meme_id').count()['chunk']
    d = {
        "meme_id": [],
        "count": [],
        "inbound_count": [],
        "outbound_count": [],
        'cluster_size': []
    }

    for meme_id in tqdm(memes_count):
        try:
            d['meme_id'].append(int(meme_id))
        except ValueError:
            continue
        d['count'].append(memes_count[meme_id])
        d['inbound_count'].append(inbound_memes_count[meme_id])
        d['outbound_count'].append(outbound_memes_count[meme_id])
        d['cluster_size'].append(meme_to_cluster_size[meme_id])
    return pd.DataFrame(d)


def preparing(df_clusters, df_aff, df_nc, conditioned=False): 

    print("Mapping chunks to memes...")# map chunks to clusters - meme id
    chunk_to_meme_dct = make_chunk_to_meme_id(df_clusters)
    chunk_to_meme = pd.DataFrame(chunk_to_meme_dct.items(),
                                 columns=['chunk', 'meme_id'])

    df_memes = merge_noun_chunks_with_affiliations(df_nc,df_aff)

    print("Preparing memes...")
    # get meme id for each chunk
    df_memes['memes'] = df_memes['noun_chunks_cleaned'].apply(
        lambda chunks: list(set(list(map(chunk_to_meme_dct.get, chunks))))) 
    df_memes.index = df_memes['paper_id']
    def get_memes_from_ids(ids):
        memes = []
        for id in ids:
            try:
                for meme in df_memes['memes'][id]:
                    memes.append(meme)
            except KeyError:
                pass
        return memes

    df_memes['inbound_memes'] = df_memes['inbound_citations'].apply(get_memes_from_ids)
    df_memes['outbound_memes'] = df_memes['outbound_citations'].apply(get_memes_from_ids)


    df_meme_stats = get_meme_statiscics(df_memes, chunk_to_meme)
    df_meme_stats.sort_values(by='count', ascending=False, inplace=True)
    # df_memes_to_remove = df_meme_stats[df_meme_stats['cluster_size']==1]
    # ids_to_remove = df_memes_to_remove[:N_TO_REMOVE]['meme_id']
    print("Removing memes:", list(ids_to_remove))
    df_memes['memes'] = df_memes['memes'].apply(lambda memes: [id for id in memes if id not in ids_to_remove])
    df_memes['inbound_memes'] = df_memes['inbound_memes'].apply(lambda memes: [id for id in memes if id not in ids_to_remove])
    df_memes['outbound_memes'] = df_memes['outbound_memes'].apply(lambda memes: [id for id in memes if id not in ids_to_remove])


    # Saving
    #print("Saving meme stats to",
    #      f'data/s2orc/meme_stats/{clusters_file_name}...')
    #df_meme_stats.to_parquet(f"data/s2orc/meme_stats/{clusters_file_name}")
    #print("Saving results...")
    columns = [
        'paper_id', 'outbound_citations', 'inbound_citations', 'institutions',
        'countries', 'types', 'unique_institutions',
        'noun_chunks_cleaned', 'memes', 'inbound_memes', 'outbound_memes',
        'year'
    ]
    if conditioned:
        columns.append('condition')
    df_out = df_memes[columns]
    #df_out.to_parquet(f'data/s2orc/results/{clusters_file_name}')
    #print("Saving chunk to meme mapping...")
    #chunk_to_meme.to_parquet(f'data/s2orc/chunk_meme_mappings/{clusters_file_name}')
    return df_out,chunk_to_meme

if __name__ == '__main__':
    typer.run(preparing)
