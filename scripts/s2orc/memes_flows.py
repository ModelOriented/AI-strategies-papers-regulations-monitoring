import pandas as pd
import typer
from collections import Counter
from tqdm import tqdm


def main(filename:str,output_path:str):
    print("Data loading ...")
    df_chunks = pd.read_parquet('data/s2orc/chunk_meme_mappings/'+filename)
    df = pd.read_parquet('data/s2orc/results/'+filename)

    meme_to_cluster_size = df_chunks.groupby('meme_id').count()['chunk']

    all_memes = df['memes'].explode()
    all_inbound_memes = df['inbound_memes'].explode()
    all_outbount_memes = df['outbound_memes'].explode()
    memes_count = Counter(all_memes)
    inbound_memes_count = Counter(all_inbound_memes)
    outbound_memes_count = Counter(all_outbount_memes)

    d = {"meme_id": [], "count": [], "inbound_count": [], "outbound_count": [], 'cluster_size': []}
    for meme_id in tqdm(memes_count):
        try:
            d['meme_id'].append(int(meme_id))
        except ValueError:
            continue
        d['count'].append(memes_count[meme_id])
        d['inbound_count'].append(inbound_memes_count[meme_id])
        d['outbound_count'].append(outbound_memes_count[meme_id])
        d['cluster_size'].append(meme_to_cluster_size[meme_id])
    df_memes = pd.DataFrame(d)

    outliers = df_memes[df_memes['cluster_size'] == 1]['meme_id']
    df_outliers = df_chunks[df_chunks['meme_id'].isin(outliers)]
    df_outliers_to_remove = df_outliers[df_outliers['chunk'].str.split(' ').str.len() < 2]
    memes_to_remove = df_outliers_to_remove['meme_id']

    df2 = df.explode("memes")
    df2 = df2[['memes', 'inbound_memes']]
    df2.dropna(subset=['memes'], inplace=True)

    df2 = df2[~df2['memes'].isin(memes_to_remove)]
    df2 = df2[df2['inbound_memes'].str.len() != 0]

    df3 = df2.explode("inbound_memes")
    out = df3.value_counts()
    out.reset_index(name='count').to_parquet(output_path+"/memes_edges.parquet")