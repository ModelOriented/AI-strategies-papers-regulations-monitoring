import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer
import typer
import os



def get_memes_with_aff(df:pd.DataFrame):
    def get_affiliated_memes(list_cit):
        tmp = [df.loc[cit]['memes'] for cit in list_cit if cit in df.index and df.loc[cit]['condition']]
        if len(tmp)>0:
            tmp = np.concatenate(tmp).ravel().tolist()
        return tmp
    aff_memes = df['outbound_citations'].apply(get_affiliated_memes)
    #aff_memes = [[df.loc[cit]['memes']*df.loc[cit]['is_big_tech'] for cit in list_cit if cit in df.index] for list_cit in df['outbound_citations']]

    return aff_memes


def clean_outbound_citations(df:pd.DataFrame):
    def get_affiliated_memes(list_cit):
        tmp = [df.loc[cit]['memes'] for cit in list_cit if cit in df.index and df.loc[cit]['unique_institutions'].shape != (0,)]
        if len(tmp)>0:
            tmp = np.concatenate(tmp).ravel().tolist()
        return tmp
    aff_memes = df['outbound_citations'].apply(get_affiliated_memes)
    #aff_memes = [[df.loc[cit]['memes']*df.loc[cit]['is_big_tech'] for cit in list_cit if cit in df.index] for list_cit in df['outbound_citations']]

    return aff_memes


def meme_score(df: pd.DataFrame, delta:float=0.0001):
    df['outbound_memes'] = clean_outbound_citations(df)

    print('OneHot Encoding ..')
    #OneHotEncoding of memes
    enc = MultiLabelBinarizer(sparse_output=True)
    memes_enc = enc.fit_transform(df['memes'])

    #OneHotEncoding of memes in cited papers
    c_enc = MultiLabelBinarizer(classes = enc.classes_, sparse_output=True)
    cited_memes_enc = c_enc.fit_transform(df['outbound_memes'])
    print('Meme score ...')
    #factors for meme score

    df['outbound_memes_condition'] = get_memes_with_aff(df)
    c_a_enc = MultiLabelBinarizer(classes = enc.classes_, sparse_output=True)
    cited_memes_aff_enc = c_a_enc.fit_transform(df['outbound_memes_condition'])

    stick2 = cited_memes_aff_enc.sum(axis=0) #sum of papers that are affiliated with BT AND cite papers with this meme 
    p = memes_enc.multiply(cited_memes_aff_enc)
    stick1 = p.sum(axis=0)#sum of papers that are affiliated with BT AND cite papers with this meme AND have this meme


    spark2 = cited_memes_enc.shape[0] - cited_memes_enc.sum(axis=0)#sum of papers that DO NOT cite papers with this meme
    #(1-cited_memes_enc).multiply(memes_enc).sum(axis=1) 
    spark1 = memes_enc.sum(axis=0)-memes_enc.multiply(cited_memes_enc).sum(axis=0) #sum of papers that DO NOT cite papers with this meme AND have this meme


    frequency = memes_enc.sum(axis=0)

    propagation_factor = np.divide(np.divide(stick1,stick2+delta),np.divide(spark1+delta,spark2+delta))


    print(np.shape(cited_memes_aff_enc))
    print({'meme_id': len(enc.classes_), 'meme_score': len(np.squeeze(np.array(np.multiply(propagation_factor,frequency)))),
                                'sticking_factor': len(np.squeeze(np.array(np.divide(stick1,stick2+delta)))),
                                'sparking_factor': len(np.squeeze(np.array(np.divide(spark1+delta,spark2+delta)))),
                                'stick1': len(np.squeeze(np.array(stick1))),
                                'stick2': len(np.squeeze(np.array(stick2))),
                                'spark1': len(np.squeeze(np.array(spark1))),
                                'spark2': len(np.squeeze(np.array(spark2)))
                                })
    df_memes = pd.DataFrame({'meme_id': enc.classes_, 'meme_score': np.squeeze(np.array(np.multiply(propagation_factor,frequency))),
                                'sticking_factor': np.squeeze(np.array(np.divide(stick1,stick2+delta))),
                                'sparking_factor': np.squeeze(np.array(np.divide(spark1+delta,spark2+delta))),
                                'stick1': np.squeeze(np.array(stick1)),
                                'stick2': np.squeeze(np.array(stick2)),
                                'spark1': np.squeeze(np.array(spark1)),
                                'spark2': np.squeeze(np.array(spark2))
                                })

    #df_c = df[df['condition'] == 1]
    #f_enc = MultiLabelBinarizer(sparse_output=True)
    #memes_enc = f_enc.fit_transform(df_c['memes'])
    #frequency_c = pd.DataFrame({'meme_id': enc.classes_, 'frequency': np.squeeze(np.array(memes_enc.sum(axis=0)))})
    #df_memes = df_memes.merge(frequency_c, how='left', on='meme_id')
   
    return df_memes



def main(df):
    df['outbound_memes'] = clean_outbound_citations(df)

    meme_score(df).merge(meme_score(df)).to_parquet(os.path.join('data/s2orc/meme_score',filename))



if __name__ == "__main__":
    typer.run(main)
