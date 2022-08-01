import os
import pandas as pd
import typer
import json


def create_openalex_dataset(path_to_filtered_files:str, output_dir:str):
    first = True
    for subdir, dirs, files in os.walk(path_to_filtered_files):
        for file in files:
            if file != 'already_processed.txt':
                full_path = os.path.join(subdir, file)
                with open(full_path) as f:
                    if first:
                        data = pd.DataFrame(json.loads(line) for line in f)
                        first = False
                    else:
                        data_temp = pd.DataFrame(json.loads(line) for line in f)
                        data = data.append(data_temp, ignore_index=True)
    data.to_parquet(os.path.join(output_dir, 'openalex_ml_dataset.parquet'))


if __name__ == '__main__':
    typer.run(create_openalex_dataset)
