import os
import pandas as pd
import typer
import json
import spacy


sp = spacy.load('en_core_web_sm')


def get_abstract(abstract_inverted_index: dict) -> str:
    abstract_index = {}
    if abstract_inverted_index is not None:
        for k, vlist in abstract_inverted_index.items():
            for v in vlist:
                abstract_index[v] = k

        abstract = ' '.join(abstract_index[k] for k in sorted(abstract_index.keys()))
        sentence = sp(abstract)
        abstract = ' '.join(word.lemma_ for word in sentence)
        return abstract
    else:
        return None

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
    abstract = []
    counts_by_year = []

    for subdir, dirs, files in os.walk(path_to_filtered_files):
        for file in files:
            if file != 'already_processed.txt' and file != 'manifest':
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
                        abstract.append(get_abstract(line['abstract_inverted_index']))
                        counts_by_year.append(line['counts_by_year'])
                        print(get_abstract(line['abstract_inverted_index']), flush=True)
                        
                        i += 1
            df = pd.DataFrame({'id': id, 'doi': doi, 'title': title, 'display_name': display_name,
                               'pubication_year': pubication_year, 'pubication_date': pubication_date, 'type': type,
                               'authorships': authorships, 'cited_by_count': cited_by_count, 'concepts': concepts,
                               'referenced_works': referenced_works, 'related_works': related_works,
                               'abstract': abstract, 'counts_by_year': counts_by_year})
            df.to_csv(os.path.join(output_dir, 'openalex_dataset.csv'))
            print('Checkpoint saved!', flush=True)

    df = pd.DataFrame({'id': id, 'doi': doi, 'title': title, 'display_name': display_name,
                       'pubication_year': pubication_year, 'pubication_date': pubication_date, 'type': type,
                       'authorships': authorships, 'cited_by_count': cited_by_count, 'concepts': concepts,
                       'referenced_works': referenced_works, 'related_works': related_works,
                       'abstract': abstract, 'counts_by_year': counts_by_year})
    df.to_parquet(os.path.join(output_dir, 'openalex_dataset.parquet'))


if __name__ == '__main__':
    typer.run(create_openalex_dataset)
