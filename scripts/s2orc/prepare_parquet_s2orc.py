import pandas as pd
from tqdm import tqdm
from glob import glob


OUT ='data/s2orc/s2orc_ai_prefiltered.parquet'

files_to_read = list(glob("mars/s2orc/metadata/ai/*.jsonl.gz"))

dfs = [pd.read_json(file, lines=True, compression=None) for file in tqdm(files_to_read)]
df = pd.concat(dfs)
df['in_citations_count'] = df['inbound_citations'].str.len()
df['out_citations_count'] = df['outbound_citations'].str.len()
df['arxiv_id'].fillna('', inplace=True)
df['arxiv_id'] = df['arxiv_id'].astype(str)
print("Number of papers:", len(df))
df = df[~df.abstract.isna()]
print("After removing papers with missing abstracts:", len(df))

df.to_parquet(OUT)
