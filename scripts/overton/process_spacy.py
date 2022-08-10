import pandas as pd
import spacy
from tqdm import tqdm 
import typer

tqdm.pandas()


def main(in_path: str, out_path:str, batch_size:int =10, spacy_model_name: str='en_core_web_md'):
    """
    This script takes output of overton preprocessing and prasing and creates basic spacy objects: nouns, noun chunks and lemmas for documents and paragraphs
    """

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

    print("Loading data...")
    df = pd.read_parquet(in_path)
    df = df[df['Text'].notna()].reset_index(drop=True)

    print("Loading spacy model...")
    en = spacy.load(spacy_model_name)
    en.remove_pipe("ner") # removing entity recognition for speed

    nouns = []
    noun_chunks = []
    lemmas = []
    k = 0
    new = 0
    try:
      out_df = pd.read_parquet(out_path)
      k = round(len(out_df)/batch_size)
      print("Resuming from " + str(k))
    except:
      print("No DF with given out_path. Creating a new one")
      out_df = pd.DataFrame([{'nouns':nouns, 
                              'noun_chunks':noun_chunks, 
                              'lemmas':lemmas}])
      new = 1


    n_batches = round(len(df)/batch_size)
    print("Number of batches " + str(n_batches))
    for i in range(k,n_batches): # we do it in batchsize
        print('Batch ' + str(i+1) +" / "+str(n_batches))
        batch = df[i*batch_size:(i+1)*batch_size]['Text']
        batch_nouns = []
        batch_noun_chunks = []
        batch_lemmas = []
        for document in batch: # for document in bacth

            doc_nouns = []
            doc_noun_chunks = []
            doc_lemmas = []
            for paragraph in document: #we get all paragraphs
                doc = (process(paragraph))

                doc_nouns.append(get_nouns(doc))

                doc_noun_chunks.append(get_chunks(doc))

                doc_lemmas.append([token.lemma_ for token in doc if not token.is_stop if not token.is_punct if token.is_alpha])

            batch_nouns.append(doc_nouns)
            batch_noun_chunks.append(doc_noun_chunks)
            batch_lemmas.append(doc_lemmas)

        batch_df = pd.DataFrame({'nouns':batch_nouns,
                                  'noun_chunks':batch_noun_chunks,
                                  'lemmas':batch_lemmas})
        if new == 0:
            out_df = pd.concat([out_df,batch_df],ignore_index = True)
        elif new == 1:
            out_df = batch_df
            new = 0

        out_df.to_parquet(out_path, index=False)

if __name__=="__main__":
    typer.run(main)
