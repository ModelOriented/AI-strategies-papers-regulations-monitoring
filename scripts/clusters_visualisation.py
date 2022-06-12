
import numpy as np
import pandas as pd
import typer

import plotly.express as px
import cuml


def main(input_path:str,output_path:str, min_cluster_size:int=15):
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
    reducer = cuml.UMAP(
        n_neighbors=20, n_components=2, min_dist=0.0)
    umap_data = reducer.fit_transform(np.stack(df2))
    df_clear['x'] = umap_data[:,0]
    df_clear['y'] = umap_data[:,1]
    print('Ploting ...')
    # Visualize clusters
    fig = px.scatter(df_clear, x='x', y='y', color='chunk_y', hover_data=['chunk_x'], hover_name='chunk_y')
    print('Saving ...')
    fig.write_html(output_path+'.html')
    fig.show()

if __name__ == "__main__":
    typer.run(main)
