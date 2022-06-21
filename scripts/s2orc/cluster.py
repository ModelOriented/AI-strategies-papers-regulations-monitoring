import pandas as pd
import typer
import os
import numpy as np

OUT_DIR_PATH = 'data/s2orc/clusterings'
IN_DIR_PATH = 'data/s2orc/embeddings'

def main(in_parquet_name: str,
         n_jobs: int = -1,
         cluster_selection_epsilons: str = ".0",
         gpu: bool = False,
         min_clust_size: int = 5,
         metric: str = 'euclidean'):
    if gpu:
        from cuml.cluster import HDBSCAN
    else:
        from hdbscan import HDBSCAN

    in_parquet_path = os.path.join(IN_DIR_PATH, in_parquet_name)
    out_path = os.path.join(OUT_DIR_PATH, in_parquet_name)
    os.makedirs(OUT_DIR_PATH, exist_ok=True)
    epsilons = [
        float(epsilon) for epsilon in cluster_selection_epsilons.split(",")
    ]
    print("-"*16)
    print("PARAMETERS")
    print("Epsilons:", epsilons)
    print("Min cluster size:", min_clust_size)
    print("Metric:", metric)
    print("-"*16)
    print("Loading parquet...")
    df = pd.read_parquet(in_parquet_path)
    print("Loaded:", len(df))
    
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
        out_path_spec, ext = os.path.splitext(out_path)
        df.to_parquet(out_path_spec + '_eps_' + str(eps) +
                        "_min_clust_size_" + str(min_clust_size) + ext)


if __name__ == '__main__':
    typer.run(main)