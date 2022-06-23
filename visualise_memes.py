import pandas as pd
import plotly.express as px
import streamlit as st
from glob import glob
import os.path
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt



files = glob("data/s2orc/meme_score/*.parquet")

if len(files)==0:
    "NO MEME SCORES!!!"
    exit()
@st.cache
def load_data(path: str):
    if path==None:
        return pd.DataFrame()
    cluster_names_file_path = os.path.join('data/s2orc/clusterings/clusters_names',os.path.basename(path))

    df = pd.read_parquet(path)
    df_names = pd.read_parquet(cluster_names_file_path)



    df = df[df['meme_score_A'] > 0]
    df = df[df['meme_score_BT'] > 0]
    df = df[df['meme_score_vanilla'] > 0]

    df['meme_score_vanilla_log'] = df['meme_score_vanilla'].apply(np.log)
    df = df.merge(df_names, on='meme_id', how='left')

    df['div'] = df['sticking_factor_BT'] / df['sticking_factor_A']
    df['log_div'] = df['div'].apply(np.log)

    return df

st.title('Memes visualisation')

file_name = st.selectbox("Select a file", files)

df = load_data(file_name)
os.path.basename(file_name)


"## Big Tech SCORE"
fig = plt.figure()
sns.histplot(
    df['meme_score_BT'], log_scale=(True, True),
)
fig

"## Academia SCORE"
fig = plt.figure()
sns.histplot(
    df['meme_score_A'], log_scale=(True, True),
)
fig

"## Top big tech memes"
st.table(
    df.sort_values(by='meme_score_BT', ascending=False)[['meme_score_BT', 'most_common']].head(10)
)

"## Top academia memes"
st.table(
    df.sort_values(by='meme_score_A', ascending=False)[['meme_score_A', 'most_common']].head(10)
)

"## Top all memes"
st.table(
    df.sort_values(by='meme_score_vanilla', ascending=False)[['meme_score_vanilla', 'most_common']].head(10)
)


st.table(df.sort_values(by='log_div', ascending=False)[['most_common', 'best_tfidf', 'log_div', 'div']].head(10))


st.table(df.sort_values(by='log_div', ascending=True)[['most_common', 'best_tfidf', 'log_div', 'div']].head(10))


fig = px.density_contour(df, y='log_div', x='meme_score_vanilla_log')
fig.update_traces(contours_coloring="fill", contours_showlabels = True)
st.plotly_chart(fig)

fig = px.scatter(df, y='log_div', x='meme_score_vanilla_log',hover_name='most_common')
st.plotly_chart(fig)


fig = px.scatter(df, x=df['sticking_factor_A'], y=df['sticking_factor_BT'], hover_name='most_common')
st.plotly_chart(fig)