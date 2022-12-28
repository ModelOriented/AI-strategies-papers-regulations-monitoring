import typer
import pandas as pd
import os
in_dir_path = 'data/s2orc/clusterings'
out_dir_path = 'data/s2orc/chunk_meme_mappings'

def main(filename:str):
    print("Processing:", filename)
    df = pd.read_parquet(os.path.join(in_dir_path, filename))
    df = df[['chunk','cluster']]
    df = df[df['cluster']!=-1]
    df.to_parquet(os.path.join(out_dir_path, filename))
    print("Done:", filename)
    
if __name__=="__main__":
    typer.run(main)
