import pandas as pd
import spacy
from tqdm import tqdm 
import typer
from typing import List
tqdm.pandas()

COLNAMES =  ['paper_id', 'doc_lens', 'nouns', 'noun_chunks', 'lemmas']

stopwords = set(["machine", "learning", "ai", "artificial", "intelligence"])

def get_chunks(doc, remove_stop_and_numbers = True):
    """Get list of noun chunks present in doc"""
    noun_chunks = []
    for chunk in doc.noun_chunks:
      if remove_stop_and_numbers:
        if not all([w.is_stop or (not w.is_alpha) or w.is_punct for w in chunk]):
          noun_chunks.append(chunk.text)
      else:
        noun_chunks.append(chunk.text)
    return noun_chunks


def get_nouns(doc)->List[str]:
    """Get list of non-stop nouns from doc"""
    nouns = [token.text
          for token in doc
          if (not token.is_stop and
              not token.is_punct and
              token.pos_ == "NOUN")]
    return nouns


def main(in_path: str, out_path:str, batch_size=1000, spacy_model_name: str='en_core_web_md'):
    print("Loading data...")
    df = pd.read_parquet(in_path)
    print("Loaded data:", len(df))
    df = df[df['abstract'].notna()]
    print("Non-empty abstracts:", len(df))
    print("Loading spacy model...")
    en = spacy.load(spacy_model_name)
    en.remove_pipe("ner") # removing entity recognition for speed

    def process(text:str):
        try:
            return en(text)
        except Exception:
            print("Error: {}".format(text))
            return []
    error = 0

    try:
      out_df = pd.read_parquet(out_path)
      print("Loaded existing data:", len(out_df))
    except:
      print("No file with given out_path. Creating a new one")
      out_df = pd.DataFrame(columns = COLNAMES)

    print("Number of batches", round(len(df)/batch_size))
    num = 0
    df = df[~df['paper_id'].isin(out_df['paper_id'])]
    print("Number of papers not processed yet:", len(df))
    b_results=[]
    for paper_id, doc in zip(df['paper_id'], en.pipe(df['abstract'])):
      try:
        b_results.append([paper_id, len(doc), get_nouns(doc), get_chunks(doc), [t.lemma_ for t in doc]])
      except Exception as e:
        error += 1
        print("Error: {}".format(e))
        continue
      num+=1
      if num%batch_size == 0:
        print("Processed {}/{} documents".format(num, len(df)))
        out_df = pd.concat([out_df, pd.DataFrame(b_results, columns=COLNAMES)])
        out_df.to_parquet(out_path)
        b_results = []
    out_df.to_parquet(out_path)

    print("Processed all successfully!")    

if __name__=="__main__":
    typer.run(main)