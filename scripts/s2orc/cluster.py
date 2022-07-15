import pandas as pd
import typer
import os
import numpy as np


def main(df:pd.DataFrame,
         n_jobs: int = -1,
         cluster_selection_epsilons: str = ".0",
         gpu: bool = False,
         min_clust_size: int = 5,
         metric: str = 'euclidean'):
    if gpu:
        from cuml.cluster import HDBSCAN
    else:
        from hdbscan import HDBSCAN


    epsilons = [
        float(epsilon) for epsilon in cluster_selection_epsilons.split(",")
    ]
    print("-"*16)
    print("PARAMETERS")
    print("Epsilons:", epsilons)
    print("Min cluster size:", min_clust_size)
    print("Metric:", metric)
    print("-"*16)
    print("Dataframe length:", len(df))
    
    for eps in epsilons:
        if gpu:
            model = HDBSCAN(cluster_selection_epsilon=eps,
                            min_cluster_size=min_clust_size,
                            metric=metric)
        else:
            model = HDBSCAN(core_dist_n_jobs=n_jobs,
                            cluster_selection_epsilon=eps,
                            min_cluster_size=min_clust_size,metric=metric)
        print("epsilon = ", eps)
        print("Clutering chunks...")
        clusters = model.fit_predict(np.stack(df['embedding']))
        df['cluster'] = clusters
        print("Outliers number:", sum(df['cluster'] == -1), "which is",
                sum(df['cluster'] == -1) / len(df))
        print("Clusters number:", len(set(clusters)))
        return df

if __name__ == '__main__':
    typer.run(main)