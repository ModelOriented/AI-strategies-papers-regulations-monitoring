import pandas as pd


from glob import glob
import os.path



rows = []
for path in glob('data/s2orc/clusterings/*.parquet'):
    print(path)
    df = pd.read_parquet(path)
    outliers =(df['cluster']==-1).sum()
    rows.append({"name": os.path.basename(path), "n_clusters": df['cluster'].nunique(), "n_outliers": outliers})

print(rows)

pd.DataFrame(rows).to_csv('cluster_stats.csv')