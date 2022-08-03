import os
import typer
import json
import pandas as pd

ROOT_DIR = 'openalex-snapshot/data/works'

ML_KEYWORDS = ['artificial intelligence', 'neural network', 'machine learning', 'expert system',
               'natural language processing', 'deep learning', 'reinforcement learning', 'learning algorithm',
               'supervised learning', 'unsupervised learning', 'intelligent agent', 'backpropagation learning',
               'backpropagation algorithm', 'long short term memory', 'autoencoder', 'q learning', 'feedforward net',
               'xgboost', 'transfer learning', 'gradient boosting', 'generative adversarial network',
               'representation learning', 'random forest', 'support vector machine', 'multiclass classification',
               'robot learning', 'graph learning', 'naive bayes classification', 'classification algorithm']


def get_abstract(abstract_inverted_index: dict) -> str:
    abstract_index = {}
    for k, vlist in abstract_inverted_index.items():
        for v in vlist:
            abstract_index[v] = k

    abstract = ' '.join(abstract_index[k] for k in sorted(abstract_index.keys()))
    return abstract


def main(output_dir: str):
    errors = 0
    n_files_processed = 0
    n_ml_papers = 0

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

    for subdir, dirs, files in os.walk(ROOT_DIR):
        for file in files:
            if file != 'manifest':
                full_path = os.path.join(subdir, file)
                with open(full_path) as f:
                    n_lines = 0
                    for line in f:
                        try:
                            paper = json.loads(line)
                            for keyword in ML_KEYWORDS:
                                words = keyword.split()
                                if paper['abstract_inverted_index'] is not None:
                                    if all(word in paper['abstract_inverted_index'] for word in words):
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
                                        n_ml_papers += 1
                                        break
                        except Exception as e:
                            print(f'Error: {e}', flush=True)
                            errors += 1
                            pass
                        n_lines += 1
                        print(
                            f'Processed {n_files_processed} files, {n_lines} lines, {errors} errors, {n_ml_papers} ML papers',
                            flush=True)

            df = pd.DataFrame({'id': id, 'doi': doi, 'title': title, 'display_name': display_name,
                               'pubication_year': pubication_year, 'pubication_date': pubication_date, 'type': type,
                               'authorships': authorships, 'cited_by_count': cited_by_count, 'concepts': concepts,
                               'referenced_works': referenced_works, 'related_works': related_works,
                               'abstract_inverted_index': abstract_inverted_index,
                               'counts_by_year': counts_by_year})
            df.to_csv(os.path.join(output_dir, 'openalex_dataset.csv'))
            print('Checkpoint saved!', flush=True)

    df = pd.DataFrame({'id': id, 'doi': doi, 'title': title, 'display_name': display_name,
                       'pubication_year': pubication_year, 'pubication_date': pubication_date, 'type': type,
                       'authorships': authorships, 'cited_by_count': cited_by_count, 'concepts': concepts,
                       'referenced_works': referenced_works, 'related_works': related_works,
                       'abstract_inverted_index': abstract_inverted_index, 'counts_by_year': counts_by_year})
    df.to_parquet(os.path.join(output_dir, 'openalex_dataset.parquet'))

    print(f'Errors: {errors}', flush=True)


if __name__ == '__main__':
    typer.run(main)
