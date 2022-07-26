import os
import jsonlines
import pandas as pd
import typer
from spacy.matcher import PhraseMatcher
import spacy.lang.en

ROOT_DIR = '/raid/shared/mair/AI-strategies-papers-regulations-monitoring/openalex-snapshot/works'

ML_KEYWORDS = ['artificial intelligence', 'machine learning', 'classifier', 'neural network', 'deep learning',
               'data science', 'nlp', 'computer vision', 'ai', 'neural net', 'natural language processing', 'cnn',
               'rnn', 'lstm', 'backpropagation', 'reinforcement learning', 'xgboost', 'random forest', 'svm',
               'decision tree', 'gradient boosting', 'bayesian network']

en = spacy.lang.en.English()


def prepare_matcher():
    matcher = PhraseMatcher(en.vocab, attr="LEMMA")
    for phrase in ML_KEYWORDS:
        matcher.add(phrase, None)
    return matcher


def get_abstract(abstract_inverted_index: dict) -> str:
    abstract_index = {}
    for k, vlist in abstract_inverted_index.items():
        for v in vlist:
            abstract_index[v] = k

    abstract = ' '.join(abstract_index[k] for k in sorted(abstract_index.keys()))
    return abstract


def main(output_dir: str):
    ml_papers = []
    matcher = prepare_matcher()
    for subdir, dirs, files in os.walk(ROOT_DIR):
        for file in files:
            with jsonlines.open(file) as reader:
                for object in reader:
                    abstract = get_abstract(object['abstract_inverted_index'])
                    to_keep = False
                    if abstract:
                        doc = en(abstract)
                        matches = matcher(doc)
                        if len(matches) > 0:
                            to_keep = True
                    if to_keep:
                        ml_papers.append(object)

    df = pd.DataFrame(ml_papers)
    df.to_parquet(output_dir)


if __name__ == '__main__':
    typer.run(main)
