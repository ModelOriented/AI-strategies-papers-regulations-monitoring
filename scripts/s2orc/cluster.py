import pandas as pd
import typer
import os
import numpy as np


def main(in_parquet_path: str, out_path: str, n_jobs:int = -1, cluster_selection_epsilons:str=".0", gpu:bool=False, min_clust_size:int=5, min_samples:str="5,7,9,11,13,15"):
    if gpu:
        from cuml.cluster import HDBSCAN
    else:
        from hdbscan import HDBSCAN
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    epsilons = [float(epsilon) for epsilon in cluster_selection_epsilons.split(",")]
    min_samp = [int(s) for s in min_samples.split(",")]
    print("Will process epsilons:", epsilons)
    print("Loading parquet...")
    df = pd.read_parquet(in_parquet_path)
    print("Loaded:",len(df))
    
    for eps in epsilons:
        for min_s in min_samp:
            if gpu:
                model = HDBSCAN(cluster_selection_epsilon=eps, min_cluster_size=min_clust_size, min_samples=min_s)
            else:
                model = HDBSCAN(core_dist_n_jobs=n_jobs, cluster_selection_epsilon=eps,min_cluster_size=min_clust_size,min_samples=min_s)
            print("epsilon = ", eps)
            print("Clutering chunks...")
            clusters = model.fit_predict(np.stack(df['embedding']))
            df['cluster'] = clusters
            print("Outliers number:", sum(df['cluster']==-1), "which is", sum(df['cluster']==-1)/len(df))
            print("Clusters number:", len(set(clusters)))
            out_path_spec, ext = os.path.splitext(out_path)
            df.to_parquet(out_path_spec+'_eps_'+str(eps)+"_min_samples_"+str(min_s)+ext)

if __name__=='__main__':
    typer.run(main)