import pandas as pd
import spacy
import typer
from spacy.language import Language
from spacy_langdetect import LanguageDetector

@Language.factory("language_detector")
def _create_language_detector(nlp: Language, name: str) -> LanguageDetector:
    """Function registered to spacy as a pipeline step"""
    return LanguageDetector(language_detection_function=None)

def process(text, en):
    try:
        doc = en(text)
        return doc,doc._.language["language"]
    except Exception:
        print("Error: {}".format(text))
        return "",""
        
def get_nouns(docs):
    """
    Desc:   Get nouns from docs list
    Input:  docs list of Doc objects        
    Output: List of nouns that are not stop words from all docs list.
    """
    nouns = [token.text
            for token in docs
            if (not token.is_stop and
                not token.is_punct and
                token.pos_ == "NOUN")]
    return nouns

def get_chunks(docs, stopwords = False):
    """
    Desc:   Prepares list of noun chunks present in all docs
    Input:  docs list of Doc objects 
            stopwords bool describving if we want to remove stopwords or not
    Output: list of noun chunks
    """
    noun_chunks = []
    for chunk in docs.noun_chunks:
        if stopwords:
            stop = True
            for w in chunk:
                if w.is_stop:
                    stop = False
            if stop:
                noun_chunks.append(chunk.text)
        else:
            noun_chunks.append(chunk.text)
    return noun_chunks

def main(in_path: str, out_path:str, global_batch_size:int =10, spacy_model_name: str='en_core_web_md'):
    """
    This script takes output of overton preprocessing and prasing and creates basic spacy objects: nouns, noun chunks and lemmas for documents and paragraphs
    """

    spacy.prefer_gpu()
    print("Loading data...")
    df = pd.read_parquet(in_path)
    print("Length with NAs:",len(df))
    df = df[df['Text'].notna()].reset_index(drop=True)
    print("Length after removing NAs:",len(df))

    print("Loading spacy model...")
    en = spacy.load(spacy_model_name)
    en.add_pipe("language_detector")
    en.remove_pipe("ner") # removing entity recognition for speed

    nouns = []
    noun_chunks = []
    lemmas = []
    merged_nouns = []
    merged_noun_chunks = []
    merged_lemmas = []

    k = 0
    new = 0
    try:
      out_df = pd.read_parquet(out_path)
      k = round(len(out_df)/global_batch_size)
      print("Resuming from " + str(k))
    except:
      print("No DF with given out_path. Creating a new one")
      out_df = pd.DataFrame([{'nouns':nouns, 
                              'noun_chunks':noun_chunks, 
                              'lemmas':lemmas,
                              'merged_nouns':merged_nouns,
                              'merged_noun_chunks':merged_noun_chunks,
                              'merged_lemmas':merged_lemmas}])
      new = 1


    n_batches = int(len(df)/global_batch_size) + 1
    print("Number of batches " + str(n_batches))
    for i in range(k,n_batches): # we do it in batchsize
        print('Batch ' + str(i+1) +" / "+str(n_batches))
        if (i+1)*global_batch_size > len(df):
            batch = df[i*global_batch_size:]['Text']
            batch_title = df[i*global_batch_size:]['Title']
        else :
            batch = df[i*global_batch_size:(i+1)*global_batch_size]['Text']
            batch_title = df[i*global_batch_size:(i+1)*global_batch_size]['Title']
        batch_nouns = []
        batch_noun_chunks = []
        batch_lemmas = []
        batch_merged_nouns = []
        batch_merged_noun_chunks = []
        batch_merged_lemmas = []
        batch_language = []

        for document in batch: # for document in batch
            doc_nouns = []
            doc_noun_chunks = []
            doc_lemmas = []
            doc_merged_nouns = []
            doc_merged_noun_chunks = []
            doc_merged_lemmas = []
            doc_language = []

            for paragraph in en.pipe(document, batch_size=50): 
                doc,lang = process(paragraph,en)

                nouns = get_nouns(doc)
                doc_nouns.append(nouns)
                chunks = get_chunks(doc)
                doc_noun_chunks.append(chunks)
                lem = [token.lemma_ for token in doc if not token.is_stop if not token.is_punct if token.is_alpha]
                doc_lemmas.append(lem)

                doc_merged_nouns += nouns
                doc_merged_noun_chunks += chunks
                doc_merged_lemmas += lem
                doc_language.append(lang)

            batch_nouns.append(doc_nouns)
            batch_noun_chunks.append(doc_noun_chunks)
            batch_lemmas.append(doc_lemmas)
            batch_merged_nouns.append(doc_merged_nouns)
            batch_merged_noun_chunks.append(doc_merged_noun_chunks)
            batch_merged_lemmas.append(doc_merged_lemmas)
            batch_language.append(doc_language)

        batch_df = pd.DataFrame({'title': batch_title,
                                'nouns':batch_nouns, 
                                'noun_chunks':batch_noun_chunks, 
                                'lemmas':batch_lemmas,
                                'merged_nouns':batch_merged_nouns,
                                'merged_noun_chunks':batch_merged_noun_chunks,
                                'merged_lemmas':batch_merged_lemmas,
                                'paragraph_language':batch_language})

        if new == 0:
            out_df = pd.concat([out_df,batch_df],ignore_index = True)
        elif new == 1:
            out_df = batch_df
            new = 0

        out_df.to_parquet(out_path, index=False)

if __name__=="__main__":
    typer.run(main)

