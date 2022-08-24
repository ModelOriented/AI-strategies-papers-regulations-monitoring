import os
import pandas as pd
import typer
import json


def create_openalex_dataset(path_to_filtered_files:str, output_dir:str):
    i = 1

    id = []
    doi = []
    title = []
    display_name =[]
    pubication_year = []
    pubication_date = []
    type = []
    authorships = []
    cited_by_count = []
    concepts = []
    referenced_works = []
    related_works = []
    abstract_inverted_index = []
    counts_by_year = []

    for subdir, dirs, files in os.walk(path_to_filtered_files):
        for file in files:
            if file != 'already_processed.txt':
                full_path = os.path.join(subdir, file)
                with open(full_path) as f:
                    for line in f:
                        line = json.loads(line)
                        print(f'{i}/{1330668}', flush=True)
                        print(line['id'], flush=True)
                        id.append(line['id'])
                        doi.append(line['doi'])
                        title.append(line['title'])
                        display_name.append(line['display_name'])
                        pubication_year.append(line['publication_year'])
                        pubication_date.append(line['publication_date'])
                        type.append(line['type'])
                        authorships.append(line['authorships'])
                        cited_by_count.append(line['cited_by_count'])
                        concepts.append(line['concepts'])
                        referenced_works.append(line['referenced_works'])
                        related_works.append(line['related_works'])
                        abstract_inverted_index.append(line['abstract_inverted_index'])
                        counts_by_year.append(line['counts_by_year'])

                        i += 1

            df = pd.DataFrame({'id': id, 'doi': doi, 'title': title, 'display_name': display_name,
                               'pubication_year': pubication_year, 'pubication_date': pubication_date, 'type': type,
                               'authorships': authorships, 'cited_by_count': cited_by_count, 'concepts': concepts,
                               'referenced_works': referenced_works, 'related_works': related_works,
                               'abstract_inverted_index': abstract_inverted_index, 'counts_by_year': counts_by_year})
            df.to_csv(os.path.join(output_dir, 'openalex_subset.csv'))
            print('Checkpoint saved!', flush=True)
            if i >= 10:
                break
        if i>= 10:
            break

    df = pd.DataFrame({'id': id[0:10], 'doi': doi[0:10], 'title': title[0:10], 'display_name': display_name[0:10],
                       'pubication_year': pubication_year[0:10], 'pubication_date': pubication_date[0:10], 'type': type[0:10],
                       'authorships': authorships[0:10], 'cited_by_count': cited_by_count[0:10], 'concepts': concepts[0:10],
                       'referenced_works': referenced_works[0:10], 'related_works': related_works[0:10],
                       'abstract_inverted_index': abstract_inverted_index[0:10], 'counts_by_year': counts_by_year[0:10]})
    df.to_parquet(os.path.join(output_dir, 'openalex_subset.parquet'))


if __name__ == '__main__':
    typer.run(create_openalex_dataset)