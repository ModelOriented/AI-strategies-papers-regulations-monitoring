import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer
import typer
import os

def add_columns(df: pd.DataFrame):
    is_academia = []
    is_company = []
    for index, paper in df.iterrows():
        if len(paper['types']) == 0:
            is_academia.append(0)
            is_company.append(0)
        else:
            paper_types = set()
            for author in paper['types']:
                for aff in author:
                    paper_types.add(aff)
            if 'company' in paper_types:
                is_company.append(1)
            else:
                is_company.append(0)
            if 'education' in paper_types or 'facility' in paper_types or 'government' in paper_types:
                is_academia.append(1)
            else:
                is_academia.append(0)

    df['is_academia'] = is_academia
    df['is_company'] = is_company
    return df


def get_memes_with_aff(df:pd.DataFrame,affiliation:str):
    def get_affiliated_memes(list_cit):
        tmp = [df.loc[cit]['memes'] for cit in list_cit if cit in df.index and df.loc[cit][affiliation]]
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


def meme_score(df: pd.DataFrame, delta=0.0001, conditioning = None):
    print('OneHot Encoding ..')
    #OneHotEncoding of memes
    enc = MultiLabelBinarizer(sparse_output=True)
    memes_enc = enc.fit_transform(df['memes'])

    #OneHotEncoding of memes in cited papers
    c_enc = MultiLabelBinarizer(classes = enc.classes_, sparse_output=True)
    cited_memes_enc = c_enc.fit_transform(df['outbound_memes'])
    print('Meme score ...')
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
        df_memes = pd.DataFrame({'meme_id': enc.classes_, 'meme_score_vanilla': np.squeeze(np.array(np.multiply(propagation_factor,frequency))),
                                 'sticking_factor_vanilla': np.squeeze(np.array(np.divide(stick1,stick2+delta))),
                                 'sparking_factor_vanilla': np.squeeze(np.array(np.divide(spark1+delta,spark2+delta))),
                                 'frequency': np.squeeze(np.array(frequency))})
    elif conditioning == 'is_big_tech':
        df_bt = df[df['is_big_tech'] == 1]
        enc_bt = MultiLabelBinarizer(sparse_output=True)
        memes_enc_bt = enc_bt.fit_transform(df_bt['memes'])
        frequency_bt = pd.DataFrame({'meme_id': enc_bt.classes_, 'frequency_bt': np.squeeze(np.array(memes_enc_bt.sum(axis=0)))})
        df_memes = pd.DataFrame({'meme_id': enc.classes_, 'meme_score_BT': np.squeeze(np.array(np.multiply(propagation_factor,frequency))),
                                 'sticking_factor_BT': np.squeeze(np.array(np.divide(stick1,stick2+delta))),
                                 'sparking_factor_BT': np.squeeze(np.array(np.divide(spark1+delta,spark2+delta))),
                                 'stick1_BT': np.squeeze(np.array(stick1)),
                                 'stick2_BT': np.squeeze(np.array(stick2)),
                                 'spark1_BT': np.squeeze(np.array(spark1)),
                                 'spark2_BT': np.squeeze(np.array(spark2))
                                 })
        df_memes = df_memes.merge(frequency_bt, how='left', on='meme_id')
    elif conditioning == 'is_company':
        df_memes = pd.DataFrame({'meme_id': enc.classes_,
                                 'meme_score_C': np.squeeze(np.array(np.multiply(propagation_factor, frequency)))})
    elif conditioning == 'is_academia':
        df_a = df[df['is_academia'] == 1]
        enc_a = MultiLabelBinarizer(sparse_output=True)
        memes_enc_a = enc_a.fit_transform(df_a['memes'])
        frequency_a = pd.DataFrame(
            {'meme_id': enc_a.classes_, 'frequency_a': np.squeeze(np.array(memes_enc_a.sum(axis=0)))})
        df_memes = pd.DataFrame({'meme_id': enc.classes_,
                                 'meme_score_A': np.squeeze(np.array(np.multiply(propagation_factor, frequency))),
                                 'sticking_factor_A': np.squeeze(np.array(np.divide(stick1, stick2 + delta))),
                                 'sparking_factor_A': np.squeeze(np.array(np.divide(spark1 + delta, spark2 + delta))),
                                 'stick1_A': np.squeeze(np.array(stick1)),
                                 'stick2_A': np.squeeze(np.array(stick2)),
                                 'spark1_A': np.squeeze(np.array(spark1)),
                                 'spark2_A': np.squeeze(np.array(spark2))
                                 })
        df_memes = df_memes.merge(frequency_a, how='left', on='meme_id')
    return df_memes



def main(filename:str, conditioning: str):
    if conditioning not in set(['is_big_tech', 'is_company', 'is_academia', 'summary']):
        raise KeyError
    df = pd.read_parquet(os.path.join('data/s2orc/results',filename))
    if conditioning in set(['is_company', 'is_academia', 'summary']):
        df = add_columns(df)
    if conditioning is not None:
        df['outbound_memes'] = clean_outbound_citations(df)
    if conditioning != 'summary':
        meme_score(df).merge(meme_score(df, conditioning=conditioning)).to_parquet(os.path.join('data/s2orc/meme_score',filename))
    else:
        res = meme_score(df).merge(meme_score(df, conditioning='is_big_tech'), on='meme_id', how='left').merge(meme_score(df, conditioning='is_academia'), on='meme_id', how='left')
        print(f'Saving in {os.path.join("data/s2orc/meme_score",filename)}')
        res.to_parquet(os.path.join('data/s2orc/meme_score',filename))


if __name__ == "__main__":
    typer.run(main)
