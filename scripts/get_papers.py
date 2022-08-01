import os
import jsonlines
import typer
import json

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
    for subdir, dirs, files in os.walk(ROOT_DIR):
        for file in files:
            print(dirs)
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
                                        os.makedirs(os.path.join(output_dir, subdir), exist_ok=True)
                                        print(os.path.join(output_dir, dirs, file))
                                        with jsonlines.open(os.path.join(output_dir, subdir, file), mode='w+', encoding='utf-8') as output_f:
                                            output_f.write(paper)
                                        n_ml_papers += 1
                                        break
                        except:
                            errors += 1
                            pass
                        n_lines += 1
                        print(
                            f'Processed {n_files_processed} files, {n_lines} lines, {errors} errors, {n_ml_papers} ML papers',
                            flush=True)

            n_files_processed += 1
            print('Processed {} files'.format(n_files_processed), flush=True)

    print(f'Errors: {errors}', flush=True)


if __name__ == '__main__':
    typer.run(main)
