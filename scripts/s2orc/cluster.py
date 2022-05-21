import pandas as pd
import hdbscan
import typer
import json

import os



def main(in_json_path: str, out_path: str, n_jobs:int = -1, cluster_selection_epsilons:str=".0"):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    epsilons = [float(epsilon) for epsilon in cluster_selection_epsilon.split(",")]
    print("Will process epsilons:", epsilons)
    print("Loading json...")
    chunk_to_embedding = json.load(open(in_json_path))
    print(len(chunk_to_embedding))
    
    for eps in epsilons:
        model = hdbscan.HDBSCAN(core_dist_n_jobs=n_jobs, cluster_selection_epsilon=eps)
        print("epsilon = ", eps)
        # print("Clutering chunks...")
        clusters = model.fit_predict(list(chunk_to_embedding.values()))
        # print("Saving data...")
        df_out = pd.DataFrame([{"cluster": cluster, "chunk":chunk, "embedding":embedding} for cluster, (chunk, embedding) in zip(clusters, chunk_to_embedding.items())])
        print("Outliers number:", sum(df_out['cluster']==-1), "which is", sum(df_out['cluster']==-1)/len(df_out))
        print("Clusters number:", len(set(clusters)))
        out_path_spec, ext = os.path.splitext(out_path)
        df_out.to_parquet(out_path_spec+'_eps_'+str(eps)+ext)

if __name__=='__main__':
    typer.run(main)