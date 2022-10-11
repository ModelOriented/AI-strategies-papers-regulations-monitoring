print('works vol 1?', flush = True)
import pandas as pd
import spacy
from tqdm import tqdm 
import typer
from spacy.language import Language
from spacy_langdetect import LanguageDetector
print('works?', flush = True)
tqdm.pandas()

@Language.factory("language_detector")
def _create_language_detector(nlp: Language, name: str) -> LanguageDetector:
    """Function registered to spacy as a pipeline step"""
    return LanguageDetector(language_detection_function = None)

def process(text, en):
    try:
        print('en', flush = True)
        print(text , flush = True)             
        doc = en(text)
        print('return', flush = True)
        return doc, doc._.language["language"]
    except Exception:
        print('error', flush = True)
        print("Error: {}".format(text))
        return "", ""
        
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

def main(in_path: str, out_path: str, batch_size: int = 10, spacy_model_name: str = 'en_core_web_md', sentences_embedding: str = 'all-MiniLM-L6-v2'):
    """
    This script takes output of overton preprocessing and prasing and creates basic spacy objects: nouns, noun chunks and lemmas for documents and paragraphs
    """

    #spacy.prefer_gpu()
    print(out_path, flush = True)
    print("Loading data...", flush = True)
    df = pd.read_parquet(in_path)
    df = df[df['text'].notna()].reset_index(drop = True)

    print("Loading spacy model...", flush = True)
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
      k = round(len(out_df) / batch_size)
      print("Resuming from " + str(k), flush = True)
    except:
      print("No DF with given out_path. Creating a new one", flush = True)
      out_df = pd.DataFrame([{'nouns': nouns, 
                              'noun_chunks': noun_chunks, 
                              'lemmas': lemmas,
                              'merged_nouns': merged_nouns,
                              'merged_noun_chunks': merged_noun_chunks,
                              'merged_lemmas': merged_lemmas}])
      new = 1


    n_batches = round(len(df) / batch_size)
    print("Number of batches " + str(n_batches), flush = True)
    for i in range(k, n_batches): # we do it in batchsize
        print('Batch ' + str(i + 1) + " / " + str(n_batches), flush = True)
        batch = df[i*batch_size: (i + 1) * batch_size]['text']
        batch_title = df[i*batch_size: (i + 1) * batch_size]['title']
        batch_nouns = []
        batch_noun_chunks = []
        batch_lemmas = []
        batch_merged_nouns = []
        batch_merged_noun_chunks = []
        batch_merged_lemmas = []
        batch_language = []

        for document in batch: # for document in batch
            print('Doc in batch loop', flush = True)
            doc_nouns = []
            doc_noun_chunks = []
            doc_lemmas = []
            doc_merged_nouns = []
            doc_merged_noun_chunks = []
            doc_merged_lemmas = []
            doc_language = []
            print(en.pipe(document, batch_size = 50), flush = True)
            idx = 0
            for paragraph in en.pipe(document, batch_size = 50): 
                idx += 1
                print(paragraph, flush = True)
                doc, lang = process(paragraph, en)
                
                print('noun', flush = True)
                nouns = get_nouns(doc)
                doc_nouns.append(nouns)
                print('chunk', flush = True)
                chunks = get_chunks(doc)
                doc_noun_chunks.append(chunks)
                print('lemma', flush = True)
                lem = [token.lemma_ for token in doc if not token.is_stop if not token.is_punct if token.is_alpha]
                doc_lemmas.append(lem)
                doc_merged_nouns = doc_merged_nouns + nouns
                doc_merged_noun_chunks = doc_merged_noun_chunks + chunks
                doc_merged_lemmas = doc_merged_lemmas + lem
                doc_language.append(lang)
                print(idx, flush = True)
            
            print('After paragraph loop', flush = True)
            batch_nouns.append(doc_nouns)
            batch_noun_chunks.append(doc_noun_chunks)
            batch_lemmas.append(doc_lemmas)
            batch_merged_nouns.append(doc_merged_nouns)
            batch_merged_noun_chunks.append(doc_merged_noun_chunks)
            batch_merged_lemmas.append(doc_merged_lemmas)
            batch_language.append(doc_language)
            
        print('Batch DataFrame creation', flush = True)
        batch_df = pd.DataFrame({'title': batch_title,
                                'nouns': batch_nouns, 
                                'noun_chunks': batch_noun_chunks, 
                                'lemmas': batch_lemmas,
                                'merged_nouns': batch_merged_nouns,
                                'merged_noun_chunks': batch_merged_noun_chunks,
                                'merged_lemmas': batch_merged_lemmas,
                                'paragraph_language': batch_language})

        if new == 0:
            out_df = pd.concat([out_df, batch_df], ignore_index = True)
        elif new == 1:
            out_df = batch_df
            new = 0
        
        print('Saving..', flush = True)
        out_df.to_parquet(out_path, index=False)

if __name__=="__main__":
    print('Does it work?', flush = True)
    typer.run(main)

