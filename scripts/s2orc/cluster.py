import pandas as pd
import hdbscan
import typer
import os


def main(in_json_path: str, out_path: str, n_jobs:int = -1, cluster_selection_epsilons:str=".0", use_gpu:bool=False):
    if use_gpu:
        from cuml.cluster import HDBSCAN
    else:
        from hdbscan import HDBSCAN
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    epsilons = [float(epsilon) for epsilon in cluster_selection_epsilons.split(",")]
    print("Will process epsilons:", epsilons)
    print("Loading parquet...")
    chunk_to_embedding = pd.read_parquet(in_json_path)
    print(len(chunk_to_embedding))
    
    for eps in epsilons:
        model = HDBSCAN(core_dist_n_jobs=n_jobs, cluster_selection_epsilon=eps)
        print("epsilon = ", eps)
        # print("Clutering chunks...")
        clusters = model.fit_predict(chunk_to_embedding['embedding'])
        # print("Saving data...")
        df_out = pd.DataFrame([{"cluster": cluster, "chunk":chunk, "embedding":embedding} for cluster, (chunk, embedding) in zip(clusters, chunk_to_embedding.items())])
        print("Outliers number:", sum(df_out['cluster']==-1), "which is", sum(df_out['cluster']==-1)/len(df_out))
        print("Clusters number:", len(set(clusters)))
        out_path_spec, ext = os.path.splitext(out_path)
        df_out.to_parquet(out_path_spec+'_eps_'+str(eps)+ext)

if __name__=='__main__':
    typer.run(main)