import pandas as pd


from glob import glob
import os.path



rows = []
for path in glob('data/s2orc/clusterings/*.parquet'):
    print(path)
    df = pd.read_parquet(path,columns=['chunk', 'cluster'])
    outliers =(df['cluster']==-1).sum()
    n_clusters = df['cluster'].nunique()-1
    mean_cluster_size = (df['cluster']==-1).sum()/n_clusters
    rows.append({"name": os.path.basename(path), "n_clusters": n_clusters, "n_outliers": outliers})

print(rows)

pd.DataFrame(rows).to_csv('cluster_stats.csv')