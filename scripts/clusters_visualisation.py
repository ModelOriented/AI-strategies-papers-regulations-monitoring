
import numpy as np
import pandas as pd
import typer

import plotly.express as px

import cuml

def main(input_path:str,output_path:str):
    #reading in data
    df = pd.read_parquet(input_path)
    #clearing data - in current dataset also 35 cluster with perccentages
    df_clear=df.query('cluster not in [-1]')

    clusters=df_clear.groupby(by=['cluster'])['chunk'].count().reset_index()

    #getting clusters' names
    '''clusters['name']=['none' for _ in range(clusters.shape[0])]
    for _, row in df_clear.iterrows():
        if clusters.loc[row['cluster']]['name'] != 'none':
            if len(row['chunk'])<len(clusters.loc[row['cluster']]['name']):
                clusters.at[row['cluster'],'name']  = row['chunk']
        else:
            clusters.at[row['cluster'],'name'] = row['chunk']
    df_clear['cluster_name'] = [clusters.loc[(int(row['cluster'])),'name'] for i, row in df_clear.iterrows()]
'''
    #getting the embedding to a Dataframe
    final_rows = []
    for _, row in df_clear.iterrows():
        embed=row['embedding']
        final_rows.append(embed)
    df2 = pd.DataFrame(final_rows)

    #UMAP
    reducer = cuml.UMAP(
        n_neighbors=20, n_components=2, min_dist=0.0, target_metric="cosine"
    )
    umap_data = reducer.fit_transform(df2)
    result = pd.DataFrame(umap_data, columns=["x", "y"])
    df_clear['x'] = umap_data[:,0]
    df_clear['y'] = umap_data[:,1] 

    # Visualize clusters
    fig = px.scatter(df_clear, x='x', y='y', color='cluster_name', hover_data=['chunk'],
                    hover_name='cluster_name')
    fig.write_html(output_path+'.html')
    fig.show()

if __name__ == "__main__":
    typer.run(main)
