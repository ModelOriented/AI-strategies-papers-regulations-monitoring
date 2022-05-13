import pandas as pd
from tqdm import tqdm
from glob import glob


files_to_read = list(glob("mars/s2orc/metadata/ai/*.jsonl.gz"))
dfs = [pd.read_json(file, lines=True, compression=None) for file in tqdm(files_to_read)]
df = pd.concat(dfs)
df['in_citations_count'] = df['inbound_citations'].str.len()

df = df[df['in_citations_count']!=0]
df = df[~df.abstract.isna()]

df.to_csv('data/s2orc/s2orc_ai_prefiltered.csv')
