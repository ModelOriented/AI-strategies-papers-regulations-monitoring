import pandas as pd
import spacy
import joblib
from tqdm import tqdm 
from collections import Counter
from sentence_transformers import SentenceTransformer

tqdm.pandas()


def main(in_path: str, out_path_lens:str, out_path_nouns:str, out_path_noun_chunks:str, 
         out_path_lemmas:str, batch_size=100, spacy_model_name: str='en_core_web_md',
          sentences_embedding:str = 'all-MiniLM-L6-v2'):
    print("Loading data...")
    df = pd.read_parquet(in_path)
    df = df[df['abstract'].notna()].reset_index(drop=True)
    df = df[:159]
    print("Loading spacy model...")
    en = spacy.load(spacy_model_name)
    en.remove_pipe("ner") # removing entity recognition for speed

    def process(text):
        try:
            return en(text)
        except Exception:
            print("Error: {}".format(text))
            return ""
            
    def get_nouns(docs):
        """
        Desc:   Get nouns from docs list
        Input:  docs list of Doc objects        
        Output: List of nouns that are not stop words from all docs list.
        """
        nouns = [token.text
              for doc in docs
              for token in doc
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
        for doc in docs:
          for chunk in doc.noun_chunks:
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

    lens = []
    nouns = []
    noun_chunks = []
    lemmas = []
    print("Number of batches",len(df)//batch_size+1)
    for i in range(len(df)//batch_size+1): # we do it in batchsize observations in a batch
        print('Batch',i+1,'/',len(df)//batch_size+1)
        docs = df['abstract'][batch_size*i:batch_size*(i+1)].apply(process)
        lens = lens + list(docs.str.len())
        nouns = nouns + get_nouns(docs)
        noun_chunks = noun_chunks + get_chunks(docs)
        lemmas = lemmas + (docs.apply(lambda doc: [token.lemma_ for token in doc if not token.is_stop if not token.is_punct if token.is_alpha])).sum()

    noun_chunks_df = pd.DataFrame(noun_chunks,columns =['NounChunk'])
    print("Saving...")
    #return lens,nouns,noun_chunks,lemmas
    joblib.dump(lens,out_path_lens)
    joblib.dump(nouns,out_path_nouns)
    noun_chunks_df.to_parquet(out_path_noun_chunks, index=False)
    joblib.dump(lemmas,out_path_lemmas)
    print('Saved!')
