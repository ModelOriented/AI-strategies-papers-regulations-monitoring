# %%
import pandas as pd
import numpy as np
from collections import Counter
import matplotlib.pyplot as plt
import re
# %%
# file from https://github.com/ModelOriented/AI-strategies-papers-regulations-monitoring/blob/master/data/s2orc/big_ai_dataset_with_affiliations_nb.parquet.dvc
df1 = pd.read_parquet('big_ai_dataset_with_affiliations_nb.parquet')
abstracts = df1[['paper_id', 'title', 'abstract']]
abstracts
# %%
# file from https://github.com/ModelOriented/AI-strategies-papers-regulations-monitoring/blob/master/data/s2orc/bt_company_meme_popularity/bt/paper_bt_chunks.pkl.dvc
chunks = pd.read_pickle('paper_bt_chunks.pkl')
chunks
# %%
def paper_analysis(id):
    noun_chunks = chunks[chunks['paper_id'] == id]['noun_chunks']
    text = abstracts[abstracts['paper_id'] == id].iloc[0,2].lower()
    if len(text) == 0:
        print('No paper for this id')
        return 0
    if len(noun_chunks.to_list()) == 0:
        print('No noun chunks for this paper!')
        return 0
    noun_chunks = noun_chunks.to_list()[0].tolist()
    noun_chunks = np.unique(noun_chunks).tolist()

    presence = []
    for i in range(len(noun_chunks)):
        k = text.find(noun_chunks[i])
        if (k >= 0):
            k = 1
        else :
            k = 0
        presence.append(k)
    print(noun_chunks)
    print(presence)
    chunk_presence = pd.DataFrame({'noun_chunks': noun_chunks, 'presence': presence})

    text_main = text
    highlight_list = noun_chunks
    highlight_str = r"\b(?:" + '|'.join(highlight_list) + r")\b"
    text_highlight = re.sub(highlight_str, '\033[91m\g<0>\033[00m', text_main)
    print(text_highlight)

    percent = sum(presence) / len(presence)
    print('Percentage of matched noun chunks:')
    print(percent)

    return(chunk_presence)
# %%
paper_analysis(3151028)

# %%
paper_analysis(20396530)

# %%
paper_analysis(4634377)
# %%
