import pandas as pd
from collections import Counter
import os.path
import numpy as np
from tqdm import tqdm
import typer


tqdm.pandas()
IN_DIR = 'data/s2orc/chunk_meme_mappings'
OUT_DIR = 'data/s2orc/clusterings/clusters_names'


#file_name = 'reduced_300_big_cleaned_mini_all-MiniLM-L6-v2_eps_0.0.parquet'

def prepare_names(chunk_meme_mappings:pd.DataFrame, processed_table:pd.DataFrame):
    chunk_meme_mappings['chunk_words'] = chunk_meme_mappings['chunk'].str.split()
    df = chunk_meme_mappings.explode('chunk_words')

    chunks_counter = Counter(processed_table['noun_chunks_cleaned'].explode())
    chunk_count = pd.Series(dict(chunks_counter))
    chunk_count = pd.DataFrame(chunk_count, columns=['count'])

    dff = pd.merge(df, chunk_count, left_on='chunk', right_index=True)
    meme_chunk_to_count = dff.groupby(['meme_id', 'chunk_words']).sum('count')

    meme_chunk_to_count = meme_chunk_to_count.reset_index()

    word_to_count = meme_chunk_to_count.groupby('chunk_words').sum()['count']
    A = meme_chunk_to_count['count'].sum() / len(meme_chunk_to_count['meme_id'].unique())

    ctfidf = meme_chunk_to_count.progress_apply(lambda x: x['count'] * np.log(1 + A  / word_to_count[x['chunk_words']]), axis=1) # calculating ctfidf

    meme_chunk_to_count['ctfidf'] = ctfidf

    meme_to_name = dict()
    print("Calculating tfidf...")
    for meme_id, g in tqdm(meme_chunk_to_count.groupby('meme_id')):
        words = g.nlargest(2, 'ctfidf')['chunk_words']
        name = "_".join(words)
        meme_to_name[meme_id] = name

    memes_with_chunks_counts = pd.merge(chunk_meme_mappings, chunk_count, left_on='chunk', right_index=True)
    memes_with_chunks_counts.index=memes_with_chunks_counts['chunk']
    chunks_groupby_meme_id = memes_with_chunks_counts.groupby('meme_id')['count'].idxmax()

    meme_to_idx = memes_with_chunks_counts.groupby('meme_id')['count'].idxmax()

    meme_to_most_common = dict()
    print("Counting most common chunks in meme...")
    for meme in tqdm(meme_to_name):
        idx = meme_to_idx[meme]
        chunk = memes_with_chunks_counts['chunk'][idx]
        meme_to_most_common[meme] = chunk

    df_out = pd.DataFrame({"most_common": meme_to_most_common, "best_tfidf":meme_to_name})
    df_out.index.name = "meme_id"
    df_out=df_out.reset_index()

    return df_out

def names(chunk_to_meme:pd.DataFrame, df_processed:pd.DataFrame):
    print("chunk_to_meme columns:", chunk_to_meme.columns)
    print("df_processed columns:", df_processed.columns)
    if chunk_to_meme.isna().sum() != 0:
        print("Warning: NaNs in chunk_to_meme teble")
    if df_processed.isna().sum() != 0:
        print("Warning: NaNs in df_processed table")

    df_out = prepare_names(chunk_to_meme, df_processed)

    return df_out

if __name__ == "__main__":
    typer.run(names)