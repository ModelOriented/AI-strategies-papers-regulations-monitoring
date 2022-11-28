import meme_score
import prepare_memes
import create_meme_names
import pandas as pd

import matplotlib.pyplot as plt

def threshold(df, percent, input_path = 'data/s2orc/processed_big.parquet',cluster_path = 'data/s2orc/clusterings/reduced_300_big_cleaned_phrase-bert_eps_0.2_min_clust_size_3.parquet'):

    df['condition']= df['company']>percent

    print('PREPARING MEMES')
    df_res = pd.read_parquet(input_path)
    df_res.index.name = None

    df_cluster = pd.read_parquet(cluster_path)
    df_cluster, chunk_to_meme = prepare_memes.preparing(df_cluster,df, df_res)

    print('CREATING NAMES')
    meme_to_name = create_meme_names.names(chunk_to_meme,df_cluster)

    print('CALCULATING MEME SCORE')
    df_meme_score = meme_score.meme_score(df_cluster)

    df_meme_score['meme_name'] = df_meme_score['meme_id'].map(meme_to_name["best_tfidf"])
    return df_meme_score



def main(df):
    for i in range(0,20):
        j=i/20
        threshold(df,j).to_parquet('data/s2orc/nb_memes/nb_memes'+str(j)+'.parquet')


if __name__ == '__main__':
    main(pd.read_parquet('data/s2orc/big_ai_dataset_with_affiliations_extended_oa.parquet'))
