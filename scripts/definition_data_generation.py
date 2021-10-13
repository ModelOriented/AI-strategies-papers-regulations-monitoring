from typing import Tuple, List

import en_core_web_lg
import pandas as pd
import spacy.tokens.doc
import typer
from spacy.tokens import DocBin
from tqdm import tqdm
import os

from mars.definition_extraction import DeftCorpusLoader

STORAGE_PATH = "mars/definition_extraction/deft_corpus/data"
positive = "DEFINITION"
negative = "NOT DEFINITION"


def transform_to_spacy3(frame: pd.DataFrame, cats: list) -> List[Tuple]:
    ret = []
    sentences = frame['Sentence'].reset_index(drop=True)
    for i in range(len(sentences)):
        ret.append((sentences[i], str(cats[i]['DEFINITION'])))
    return ret


def main():
    """
        Generate data from data from deft corpus. On basis of:
        https://github.com/Elzawawy/DeftEval
    """
    os.chdir('../')
    print("Initializing...")
    deft_loader = DeftCorpusLoader(STORAGE_PATH)
    trainframe, devframe = deft_loader.load_classification_data(preprocess=True, clean=True)
    train_cats = [{positive: bool(y), negative: not bool(y)} for y in trainframe["HasDef"]]
    dev_cats = [{positive: bool(y), negative: not bool(y)} for y in devframe["HasDef"]]

    print("Transforming to spacy3 format")
    train_df = transform_to_spacy3(trainframe, train_cats)
    dev_df = transform_to_spacy3(devframe, dev_cats)

    def make_docs(data: List[Tuple[str, str]]) -> List[spacy.tokens.doc.Doc]:
        """
        this will take a list of texts and labels
        and transform them in spacy documents

        data: list(tuple(text, label))

        returns: List(spacy.Doc.doc)
        """
        docs = []
        nlp = en_core_web_lg.load()
        for doc, label in tqdm(nlp.pipe(data, as_tuples=True), total=len(data)):
            # we need to set the (text)cat(egory) for each document
            if label == 'True':
                doc.cats["Definition"] = 1
                doc.cats["Not Definition"] = 0
            else:
                doc.cats["Definition"] = 0
                doc.cats["Not Definition"] = 1

            docs.append(doc)
        return docs

    print("Creating docs and saving in data/ folder")
    train_docs = make_docs(train_df)
    doc_bin = DocBin(docs=train_docs)
    doc_bin.to_disk("data/definition_data/trainn.spacy")

    dev_docs = make_docs(dev_df)
    doc_bin = DocBin(docs=dev_docs)
    doc_bin.to_disk("data/definition_data/devn.spacy")


if __name__ == "__main__":
    typer.run(main)