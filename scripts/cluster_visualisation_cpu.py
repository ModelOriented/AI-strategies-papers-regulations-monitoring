
import numpy as np
import pandas as pd
import typer

import plotly.express as px

import umap.umap_ as umap


def main(input_path:str, output_path:str, min_cluster_size:int=15):
    print("Data loading ...")
    # reading in data
    df = pd.read_parquet(input_path)
    # clearing data - in current dataset also 35 cluster with perccentages
    df_clear = df.query('cluster not in [-1]')
    clusters = df_clear.groupby(by=['cluster'])['chunk'].first().reset_index()
    print('Labels ...')
    df_clear = pd.merge(df_clear, clusters, on='cluster', how='left')
    df_cluster_sizes = df_clear['cluster'].value_counts().sort_values(ascending=False)
    indexes_to_plot = list(df_cluster_sizes[df_cluster_sizes > min_cluster_size].index.values)
    df_clear = df_clear[df_clear['cluster'].isin(indexes_to_plot)]
    # UMAP
    print('UMAP ...')
    df2 = df_clear['embedding']
    reducer = umap.UMAP(
        n_neighbors=20, n_components=2, min_dist=0.0)
    umap_data = reducer.fit_transform(np.stack(df2))
    df_clear['x'] = umap_data[:,0]
    df_clear['y'] = umap_data[:,1]
    print('Ploting ...')
    # Visualize clusters
    df_clear['cluster_name'] = df_clear['chunk_y']
    df_clear['noun_chunk'] = df_clear['chunk_x']

    df_clear = df_clear[['x', 'y', 'cluster_name', 'noun_chunk']]
    fig = px.scatter(df_clear, x='x', y='y', color='cluster_name', hover_data=['noun_chunk'], hover_name='cluster_name')
    print('Saving ...')
    df_clear.to_parquet(output_path+'.parquet')


if __name__ == "__main__":
    typer.run(main)