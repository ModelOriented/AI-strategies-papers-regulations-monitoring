import pandas as pd
import spacy
from tqdm import tqdm 
import typer
from collections import Counter
from sentence_transformers import SentenceTransformer

tqdm.pandas()


def main(in_path: str, out_path:str, batch_size=1000, spacy_model_name: str='en_core_web_md',
          sentences_embedding:str = 'all-MiniLM-L6-v2'):
    print("Loading data...")
    df = pd.read_parquet(in_path)
    df = df[df['abstract'].notna()].reset_index(drop=True)
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
    error = 0

    try:
      out_df = pd.read_parquet(out_path)
    except:
      print("No DF with given out_path. Creating a new one")
      out_df = pd.DataFrame([{'lens':lens, 'nouns':nouns, 'noun_chunks':noun_chunks, 'lemmas':lemmas}])

    print("Number of batches",round(len(df)/batch_size))
    for i in range(round(len(df)/batch_size)): # we do it in batchsize observations in a batch
        if len(out_df['lens'][0]) > batch_size*i:  # if we have it, it means there was an error so we want to load lists once
          if error == 0 and len(out_df['lens'][0]) != 0:
            if len(out_df['lens'][0]) == len(df):
              print('DataSet already saved')
              return 0
            else:
              print("Error occured before. Loading data from backup...")
              error = 1
              lens = list(out_df['lens'][0])
              nouns = list(out_df['nouns'][0])
              noun_chunks = list(out_df['noun_chunks'][0])
              lemmas = list(out_df['lemmas'][0])
          else:
            pass
        else :
          print('Batch',i+1,'/',round(len(df)/batch_size))
          docs = df['abstract'][batch_size*i:batch_size*(i+1)].apply(process)
          lens = lens + list(docs.str.len())
          nouns = nouns + get_nouns(docs)
          noun_chunks = noun_chunks + get_chunks(docs)
          lemmas = lemmas + (docs.apply(lambda doc: [token.lemma_ for token in doc if not token.is_stop if not token.is_punct if token.is_alpha])).sum()
          del docs
          print("Saving batch...")
          out_df = pd.DataFrame([{'lens':lens, 'nouns':nouns, 'noun_chunks':noun_chunks, 'lemmas':lemmas}])
          out_df.to_parquet(out_path, index=False)
          print('Batch saved!')
    print(len(out_df['lens'][0])," ",len(df) )
    if len(out_df['lens'][0]) == len(df):
      print('Pocess ended succesfully')
    
if __name__=="__main__":
    typer.run(main)