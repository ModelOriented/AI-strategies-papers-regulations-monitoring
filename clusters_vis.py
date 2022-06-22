import pandas as pd
import plotly.express as px
from pyparsing import col
import streamlit as st
from glob import glob
import os.path


files = glob("data/s2orc/clusts_vis/*.parquet")


@st.cache
def load_data(file_name:str):
    df = pd.read_parquet(file_name, columns=['x', 'y', 'noun_chunk','cluster_name'])
    return df

st.title('Clusters visualization')

file_name= st.selectbox("Select a file", files)

df = load_data(file_name)
st.text(os.path.basename(file_name))
noun_chunks = df['noun_chunk']
clusters_names = df['cluster_name'].unique()
# st.table(df['cluster'].value_counts().sort_values(ascending=False))
# st.table(clusters_names)
# st.plotly_chart(px.scatter(df, x='x', y='y', color='chunk_y', hover_data=['chunk_x'], hover_name='chunk_y'))

clust_names = st.multiselect('Select cluster', clusters_names)


df2 = df[df['cluster_name'].isin(clust_names)]


st.plotly_chart(px.scatter(df2, x='x', y='y', color='cluster_name', hover_data=['noun_chunk'], hover_name='cluster_name'))
