import os
import typer
import json
import jsonlines
import spacy
spacy.load('en_core_web_sm')
import spacy.lang.en
from spacy.matcher import PhraseMatcher

ROOT_DIR = 'openalex-snapshot/data/works'
en = spacy.lang.en.English()

ML_KEYWORDS = ['artificial intelligence', 'neural network', 'machine learning', 'expert system',
               'natural language processing', 'deep learning', 'reinforcement learning', 'learning algorithm',
               'supervised learning', 'unsupervised learning', 'intelligent agent', 'backpropagation learning',
               'backpropagation algorithm', 'long short term memory', 'autoencoder', 'q learning', 'feedforward net',
               'xgboost', 'transfer learning', 'gradient boosting', 'generative adversarial network',
               'representation learning', 'random forest', 'support vector machine', 'multiclass classification',
               'robot learning', 'graph learning', 'naive bayes classification', 'classification algorithm']

def prepare_matcher():
    matcher = PhraseMatcher(en.vocab, attr="NORM")  # TODO: change to lemma
    for pattern in ML_KEYWORDS:
        matcher.add(pattern, None, en(pattern))
    return matcher

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
    matcher = prepare_matcher()
    with open(os.path.join(output_dir, 'already_processed.txt')) as file:
        lines = file.readlines()
        already_processed = [line.rstrip() for line in lines]
    for subdir, dirs, files in os.walk(ROOT_DIR):
        for file in files:
            if file in already_processed:
                continue
            else:
                update_dir = subdir.split('/')[-1]
                if file != 'manifest':
                    full_path = os.path.join(subdir, file)
                    with open(full_path) as f:
                        n_lines = 0
                        for line in f:
                            try:
                                paper = json.loads(line)
                                if paper['abstract_inverted_index'] is not None:
                                    abstract = get_abstract(paper['abstract_inverted_index'])
                                    abstract = abstract.lower()
                                    if matcher(en(abstract)):
                                        os.makedirs(os.path.join(output_dir, update_dir), exist_ok=True)
                                        filename = os.path.join(output_dir, update_dir, file)
                                        if not os.path.exists(filename):
                                            with jsonlines.open(filename, mode='w') as f:
                                                f.write(paper)
                                        else:
                                            with jsonlines.open(filename, mode='a') as output_f:
                                                output_f.write(paper)
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

                n_files_processed += 1
                print('Processed {} files'.format(n_files_processed), flush=True)
                with open(os.path.join(output_dir, 'already_processed.txt'), 'a') as f:
                    f.write(os.path.join(subdir, file) + '\n')

    print(f'Errors: {errors}', flush=True)


if __name__ == '__main__':
    typer.run(main)