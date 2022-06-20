import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer
import typer
import itertools

def get_memes_with_aff(df:pd.DataFrame,affiliation:str):
    def get_affiliated_memes(list_cit):
        tmp = [df.loc[cit]['memes'] for cit in list_cit if cit in df.index and df.loc[cit][affiliation]]
        if len(tmp)>0:
            tmp = np.concatenate(tmp).ravel().tolist()
        return tmp
    aff_memes = df['outbound_citations'].apply(get_affiliated_memes)
    #aff_memes = [[df.loc[cit]['memes']*df.loc[cit]['is_big_tech'] for cit in list_cit if cit in df.index] for list_cit in df['outbound_citations']]

    return aff_memes

def meme_score(df: pd.DataFrame, delta=0.0001, conditioning = None):

    #OneHotEncoding of memes
    enc = MultiLabelBinarizer(sparse_output=True)
    memes_enc = enc.fit_transform(df['memes'])

    #OneHotEncoding of memes in cited papers
    c_enc = MultiLabelBinarizer(classes = enc.classes_, sparse_output=True)
    cited_memes_enc = c_enc.fit_transform(df['outbound_memes'])

    #factors for meme score
    if conditioning == None: 
        stick2 = cited_memes_enc.sum(axis=0) #sum of papers, that cite paper with this meme
        p = memes_enc.multiply(cited_memes_enc) #papers that cite paper with this meme AND have this meme themselves
        stick1 = p.sum(axis=0)
    else:
        df['outbound_memes_BT'] = get_memes_with_aff(df,conditioning)
        c_a_enc = MultiLabelBinarizer(classes = enc.classes_, sparse_output=True)
        cited_memes_aff_enc = c_a_enc.fit_transform(df['outbound_memes_BT'])

        stick2 = cited_memes_aff_enc.sum(axis=0) #sum of papers that are affiliated with BT AND cite papers with this meme 
        p = memes_enc.multiply(cited_memes_aff_enc)
        stick1 = p.sum(axis=0)#sum of papers that are affiliated with BT AND cite papers with this meme AND have this meme
    #elif conditioning == 'not_big_tech':

    spark2 = cited_memes_enc.shape[1] - cited_memes_enc.sum(axis=0)#sum of papers that DO NOT cite papers with this meme
    #(1-cited_memes_enc).multiply(memes_enc).sum(axis=1) 
    spark1 = memes_enc.sum(axis=0)-memes_enc.multiply(cited_memes_enc).sum(axis=0) #sum of papers that DO NOT cite papers with this meme AND have this meme


    frequency = memes_enc.sum(axis=0)
    propagation_factor = np.divide(np.divide(stick1,stick2+delta),np.divide(spark1+delta,spark2+delta))
    if conditioning == None:
        df_memes = pd.DataFrame({'meme_id': enc.classes_, 'meme_score': np.squeeze(np.array(np.multiply(propagation_factor,frequency)))})
    elif conditioning == 'is_big_tech':
        df_memes = pd.DataFrame({'meme_id': enc.classes_, 'meme_score_BT': np.squeeze(np.array(np.multiply(propagation_factor,frequency)))})

    return df_memes


def main(path:str,output_path:str):
    df= pd.read_parquet(path)

    meme_score(df).merge(meme_score(df,conditioning='is_big_tech')).to_parquet(output_path)


if __name__ == "__main__":
    typer.run(main)
