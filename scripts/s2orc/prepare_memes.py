from mars.utils import set_root_path
import pandas as pd

import os
from collections import Counter
import typer
from tqdm import tqdm

MERGED_DATA_PATH = "data/s2orc/clean_merged.parquet"
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


def get_merged_data():
    if not os.path.exists(MERGED_DATA_PATH):
        df = pd.read_parquet('data/s2orc/processed_big.parquet')
        df2 = pd.read_parquet(
            'data/s2orc/big_ai_dataset_with_affiliations.parquet')
        memes_df = pd.merge(df2,
                            df[['paper_id', 'noun_chunks_cleaned']],
                            on='paper_id',
                            how='left')
        #df2 = df2[df2['institutions'].str.len()!=0]
        memes_df['inbound_citations'] = memes_df['inbound_citations'].apply(
            lambda ids: [int(id) for id in ids])
        memes_df['outbound_citations'] = memes_df['outbound_citations'].apply(
            lambda ids: [int(id) for id in ids])

        memes_df = memes_df[~memes_df['noun_chunks_cleaned'].isna()]
        memes_df.to_parquet(MERGED_DATA_PATH)
    else:
        print(f"Loading {MERGED_DATA_PATH}...")
        memes_df = pd.read_parquet(MERGED_DATA_PATH)

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


def main(clusters_file_name: str
         ):  #reduced_300_big_cleaned_mini_all-MiniLM-L6-v2_eps_0.0.parquet
    df_memes = get_merged_data()
    # map chunks to clusters
    print("Loading clusters...")
    df_clusters = pd.read_parquet(
        f'data/s2orc/clusterings/{clusters_file_name}',
        columns=['chunk', 'cluster'])
    print("Mapping chunks to memes...")
    chunk_to_meme_dct = make_chunk_to_meme_id(df_clusters)
    chunk_to_meme = pd.DataFrame(chunk_to_meme_dct.items(),
                                 columns=['chunk', 'meme_id'])

    print("Preparing memes...")
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
    df_memes_to_remove = df_meme_stats[df_meme_stats['cluster_size']==1]
    ids_to_remove = df_memes_to_remove[:N_TO_REMOVE]['meme_id']
    print("Removing memes:", list(ids_to_remove))
    df_memes['memes'] = df_memes['memes'].apply(lambda memes: [id for id in memes if id not in ids_to_remove])
    df_memes['inbound_memes'] = df_memes['inbound_memes'].apply(lambda memes: [id for id in memes if id not in ids_to_remove])
    df_memes['outbound_memes'] = df_memes['outbound_memes'].apply(lambda memes: [id for id in memes if id not in ids_to_remove])



    # Saving
    print("Saving meme stats to",
          f'data/s2orc/meme_stats/{clusters_file_name}...')
    df_meme_stats.to_parquet(f"data/s2orc/meme_stats/{clusters_file_name}")
    print("Saving results...")
    df_out = df_memes[[
        'paper_id', 'outbound_citations', 'inbound_citations', 'institutions',
        'countries', 'types', 'unique_institutions', 'is_big_tech',
        'noun_chunks_cleaned', 'memes', 'inbound_memes', 'outbound_memes',
        'year'
    ]]
    df_out.to_parquet(f'data/s2orc/results/{clusters_file_name}')
    print("Saving chunk to meme mapping...")
    chunk_to_meme.to_parquet(f'data/s2orc/chunk_meme_mappings/{clusters_file_name}')


if __name__ == '__main__':
    typer.run(main)
