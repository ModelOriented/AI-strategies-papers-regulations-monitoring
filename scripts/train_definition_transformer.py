import transformers
import tqdm
import shutil
import tensorflow as tf

from typing import Tuple, List

import en_core_web_lg
import pandas as pd
import spacy.tokens.doc
import typer
from spacy.tokens import DocBin
from tqdm import tqdm
import os
import numpy as np


from mars.definition_extraction import DeftCorpusLoader

import tensorflow as tf
from transformers import TFRobertaForSequenceClassification

from transformers import AutoTokenizer



def transform_to_spacy3(frame: pd.DataFrame, cats: list) -> List[Tuple]:
    ret = []
    sentences = frame['Sentence'].reset_index(drop=True)
    for i in range(len(sentences)):
        if cats[i]['DEFINITION'] is True:
            cat = 1
        else:
            cat = 0

        ret.append((sentences[i], cat))
    return ret


def create_from_wiki(path='../mars/definition_extraction/wcl_datasets_v1.2/wikipedia/', files=None) -> List[Tuple]:
    if files is None:
        files = {'wiki_bad.txt': 0, 'wiki_good.txt': 1}

    file_sentences = {}

    for f in files.keys():
        filename = os.path.join(path, f)
        defs = []
        with open(filename, 'rb') as file:
            lines = file.readlines()
            lines = np.array([line.rstrip() for line in lines])

        for line in lines[1::2]:
            defs.append(str(line)[2:].split(':')[0])

        lines = lines[::2]
        for i, line in enumerate(lines):
            lines[i] = str(line)[4:-2]
        lines = lines.astype(str)

        for i, line in enumerate(lines):
            lines[i] = lines[i].replace('TARGET', defs[i])

        lines = list(lines)
        lines = [(str(line), int(files[f])) for line in lines]

        file_sentences[f] = lines

    file_sentences = list(file_sentences.values())[0] + list(file_sentences.values())[1]
    return file_sentences


def filter_out(sentences: List[Tuple], max_length: int) -> List[Tuple]:
    res = []
    for x in sentences:
        if len(x[0]) <= max_length:
            res.append(x)
    return res


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

def main():

    TRANSFORMER = "roberta-base"
    tokenizer = AutoTokenizer.from_pretrained(TRANSFORMER)

    STORAGE_PATH = "../mars/definition_extraction/deft_corpus/data"
    positive = "DEFINITION"
    negative = "NOT DEFINITION"

    print("Initializing...")
    deft_loader = DeftCorpusLoader(STORAGE_PATH)
    trainframe, devframe, testframe = deft_loader.load_classification_data(preprocess=True, clean=True)
    train_cats = [{positive: bool(y), negative: not bool(y)} for y in trainframe["HasDef"]]
    dev_cats = [{positive: bool(y), negative: not bool(y)} for y in devframe["HasDef"]]
    test_cats = [{positive: bool(y), negative: not bool(y)} for y in testframe["HasDef"]]

    print("Transforming to spacy3 format")
    wiki = create_from_wiki()
    train_df = transform_to_spacy3(trainframe, train_cats)
    dev_df = transform_to_spacy3(devframe, dev_cats)
    test_df = transform_to_spacy3(testframe, test_cats)

    train_df = wiki[:int((3 / 4 * len(wiki)))] + train_df
    dev_df = wiki[int((3 / 4 * len(wiki))):] + dev_df

    train_sentences, train_labels = list(trainframe["Sentence"]), list(trainframe["HasDef"])
    val_sentences, val_labels = list(devframe["Sentence"]), list(devframe["HasDef"])
    test_sentences, test_labels = list(testframe["Sentence"]), list(testframe["HasDef"])

    train_encodings = tokenizer(train_sentences, truncation=True, padding=True)
    val_encodings = tokenizer(val_sentences, truncation=True, padding=True)
    test_encodings = tokenizer(test_sentences, truncation=True, padding=True)

    model = TFRobertaForSequenceClassification.from_pretrained(TRANSFORMER)

    optimizer = tf.keras.optimizers.Adam(learning_rate=5e-5)
    model.compile(optimizer=optimizer, loss=model.compute_loss)  # can also use any keras loss fn

    train_dataset = tf.data.Dataset.from_tensor_slices((
        dict(train_encodings),
        train_labels
    ))
    val_dataset = tf.data.Dataset.from_tensor_slices((
        dict(val_encodings),
        val_labels
    ))
    test_dataset = tf.data.Dataset.from_tensor_slices((
        dict(test_encodings),
        test_labels
    ))

    model = TFRobertaForSequenceClassification.from_pretrained(TRANSFORMER)

    optimizer = tf.keras.optimizers.Adam(learning_rate=5e-5)
    model.compile(optimizer=optimizer, loss=model.compute_loss)

    model.fit(train_dataset.shuffle(1000).batch(16),
              validation_data=val_dataset.shuffle(1000).batch(16),
              epochs=5,
              batch_size=16)
    
    model.save('../models/' + TRANSFORMER)

if __name__ == "__main__":
    typer.run(main)