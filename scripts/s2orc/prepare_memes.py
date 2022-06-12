from mars.utils import set_root_path
import pandas as pd

import os

import typer

MERGED_DATA_PATH = "data/s2orc/clean_merged.parquet"


def make_chunk_to_meme_id(df_clusters)->dict:
    """Map all chunks to their meme id, dependent on cluster. Outliers are given new ids."""
    next_id = max(df_clusters['cluster'])+1
    chunk_to_meme_id=dict()
    for _, r in df_clusters.iterrows():
        id = r['cluster']
        if id==-1:
            id = next_id
            next_id += 1
        chunk_to_meme_id[r['chunk']] = id
    return chunk_to_meme_id


def main(clusters_file_name: str): #reduced_300_big_cleaned_mini_all-MiniLM-L6-v2_eps_0.0.parquet
    if not os.path.exists(MERGED_DATA_PATH):
        df = pd.read_parquet('data/s2orc/processed_big.parquet')
        df2 = pd.read_parquet('data/s2orc/big_ai_dataset_with_affiliations.parquet')
        memes_df = pd.merge(df2, df[['paper_id','noun_chunks_cleaned']], on='paper_id', how='left')
        #df2 = df2[df2['institutions'].str.len()!=0]
        memes_df['inbound_citations'] = memes_df['inbound_citations'].apply(lambda ids: [int(id) for id in ids])
        memes_df['outbound_citations'] = memes_df['outbound_citations'].apply(lambda ids: [int(id) for id in ids])

        memes_df = memes_df[~memes_df['noun_chunks_cleaned'].isna()]
        memes_df.to_parquet(MERGED_DATA_PATH)
    else:
        print("Loading",MERGED_DATA_PATH,"...")
        memes_df = pd.read_parquet(MERGED_DATA_PATH)


    # map chunks to clusters
    df_clusters = pd.read_parquet('data/s2orc/clusterings/'+clusters_file_name, columns=['chunk', 'cluster'])
    print("loaded!")
    chunk_to_meme = make_chunk_to_meme_id(df_clusters)
    print("processing...")
    memes_df['memes'] = memes_df['noun_chunks_cleaned'].apply(lambda chunks: list(set(list(map(chunk_to_meme.get, chunks)))))

    memes_df.index = memes_df['paper_id']

    def get_memes_from_ids(ids):
        memes = []
        for id in ids:
            try:
                for meme in memes_df['memes'][id]:
                    memes.append(meme)
            except KeyError:
                pass
        return memes

    memes_df['inbound_memes']=memes_df['inbound_citations'].apply(get_memes_from_ids)
    memes_df['outbound_memes']=memes_df['outbound_citations'].apply(get_memes_from_ids)


    df_out = memes_df[['paper_id', 'outbound_citations','inbound_citations','institutions','countries','types','unique_institutions','is_big_tech','noun_chunks_cleaned','memes', 'inbound_memes', 'outbound_memes']]
    print("saving...")
    df_out.to_parquet('data/s2orc/results/'+clusters_file_name)
    
    chunk_to_meme = pd.DataFrame(chunk_to_meme.items(), columns=['chunk','meme_id'])
    chunk_to_meme.to_parquet('data/s2orc/chunk_meme_mappings/'+clusters_file_name)


if __name__ == '__main__':
    typer.run(main)