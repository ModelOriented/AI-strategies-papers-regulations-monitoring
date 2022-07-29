import os
import jsonlines
import pandas as pd
import typer
import json

ROOT_DIR = 'openalex-snapshot/data/works'

ML_KEYWORDS = ['artificial intelligence', 'neural network', 'machine learning', 'expert system',
               'natural language processing', 'deep learning', 'reinforcement learning', 'learning algorithm',
               'supervised learning', 'unsupervised learning' , 'intelligent agent', 'backpropagation learning',
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
    ml_papers = []
    for subdir, dirs, files in os.walk(ROOT_DIR):
        for file in files:
            if file != 'manifest':
                with open(os.path.join(subdir, file)) as f:
                    data = [json.loads(line) for line in f]
                    for object in data:
                        for keyword in ML_KEYWORDS:
                            words = keyword.split()
                            if object['abstract_inverted_index'] is not None:
                                if all(word in object['abstract_inverted_index'] for word in words):
                                    ml_papers.append(object)
                                    break

    df = pd.DataFrame(ml_papers)
    df.to_parquet(output_dir)


if __name__ == '__main__':
    typer.run(main)
