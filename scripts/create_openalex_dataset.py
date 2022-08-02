import os
import pandas as pd
import typer
import json


def create_openalex_dataset(path_to_filtered_files:str, output_dir:str):
    i = 1
    jsons = []
    for subdir, dirs, files in os.walk(path_to_filtered_files):
        for file in files:
            print(f'{i}/{1330668}', flush=True)
            if file != 'already_processed.txt':
                full_path = os.path.join(subdir, file)
                with open(full_path) as f:
                    for line in f:
                        paper = json.loads(line)
                        jsons.append(paper)
                        i += 1
            if i % 1000 == 0:
                df = pd.DataFrame(jsons)
                df.to_parquet(os.path.join(output_dir, 'openalex_ml_dataset.parquet'))


if __name__ == '__main__':
    typer.run(create_openalex_dataset)
