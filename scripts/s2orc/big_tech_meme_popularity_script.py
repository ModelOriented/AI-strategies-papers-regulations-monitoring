import pandas as pd
import numpy as np
import pickle
from collections import Counter
import typer

def main(cluster_name : str, chunk_meme : str, processed_big : str, bt_perc: str):
    '''
    cluster_name : cluster_names/reduced_300_big_bleaned_phrase-bert_eps_0.2_min_clust_size_3
    chunk_meme : chunk_meme_mappings/reduced_300_big_bleaned_phrase-bert_eps_0.2_min_clust_size_3
    processed_big : processed_big
    bt_perc : big_ai_dataset_with_affiliations_nb
    '''
    print('Reading the files..', flush = True)
    cluster_name = pd.read_parquet(cluster_name)
    chunk_meme = pd.read_parquet(chunk_meme)
    processed_big = pd.read_parquet(processed_big)
    bt_perc = pd.read_parquet(bt_perc)
    
    print('Sample dataset observations..', flush = True)
    print(cluster_name.head(), flush = True)
    print(chunk_meme.head(), flush = True)
    print(processed_big.head(), flush = True)
    print(bt_perc.head(), flush = True)
    
    print('Reducing information about papers (bt_perc)..', flush = True)
    bt_perc_reduced = bt_perc[['paper_id', '%BT']].reset_index()

    print('Reducing information from processed_big..', flush = True)
    processed_reduced = processed_big[['paper_id', 'noun_chunks_cleaned']]

    print('Binning BT percantage..', flush = True)
    for i in range(len(bt_perc_reduced)):
        bt = bt_perc_reduced['%BT'][i]
        if (bt == 0):
            bt = 0
        elif (bt <= 0.05):
            bt = 0.05
        elif (bt <= 0.1):
            bt = 0.1
        elif (bt <= 0.15):
            bt = 0.15
        elif (bt <= 0.2):
            bt = 0.2
        elif (bt <= 0.25):
            bt = 0.25
        elif (bt <= 0.3):
            bt = 0.3
        elif (bt <= 0.35):
            bt = 0.35
        elif (bt <= 0.4):
            bt = 0.4
        elif (bt <= 0.45):
            bt = 0.45
        elif (bt <= 0.5):
            bt = 0.5
        elif (bt <= 0.55):
            bt = 0.55
        elif (bt <= 0.6):
            bt = 0.6
        elif (bt <= 0.65):
            bt = 0.65
        elif (bt <= 0.7):
            bt = 0.7
        elif (bt <= 0.75):
            bt = 0.75
        elif (bt <= 0.8):
            bt = 0.8
        elif (bt <= 0.85):
            bt = 0.85
        elif (bt <= 0.9):
            bt = 0.9
        elif (bt <= 0.95):
            bt = 0.95
        else :
            bt = 1
        bt_perc_reduced['%BT'][i] = bt
    
    print('Saving reduced dataframe..', flush = True)
    bt_perc_reduced.to_parquet('bt_perc_reduced.parquet')

    print('Merging bt_perc_reduced and processed_reduced into paper_bt_chunks..', flush = True)
    paper_bt_chunks = pd.merge(bt_perc_reduced, processed_reduced, on = 'paper_id', how = 'inner')[['paper_id', '%BT','noun_chunks_cleaned']]
    paper_bt_chunks.columns = ['paper_id', '%BT', 'noun_chunks']

    if len(pd.unique(paper_bt_chunks['paper_id'])) != len(paper_bt_chunks['paper_id']) :
        print('Duplicate papers appear in paper_bt_chunks', flush = True)

    print('Merging chunk_meme and cluster_name..', flush = True)
    chunk_meme_name = pd.merge(chunk_meme, cluster_name, on = 'meme_id', how = 'inner') [['chunk', 'most_common']]
    chunk_meme_name.columns = ['noun_chunk', 'meme_name']

    print('Creating aggregated_BT_chunks from paper_bt_chunks..', flush = True)
    perc_BT = [0, 0.05, 0.1, 0.15, 0.2, 0.25,  0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1]
    x = []
    agg_chunks = [np.asarray(x) for i in range(21)]

    for i in range(len(paper_bt_chunks)):
        for j in np.arange(0, 1.05, 0.05):
            if paper_bt_chunks['%BT'][i] == round(j,3):
                agg_chunks[int(j*20)] = np.concatenate((agg_chunks[int(j*20)], paper_bt_chunks['noun_chunks'][i]))
                break

    aggregated_BT_chunks = pd.DataFrame({'%BT': perc_BT, 'noun_chunks': agg_chunks})
    # to_parquet didnt work

    def save_object(obj, filename):
        with open(filename, 'wb') as outp:  # Overwrites any existing file.
            pickle.dump(obj, outp, pickle.HIGHEST_PROTOCOL)

    print('Saving aggregated_BT_chunks into pkl..', flush = True)
    save_object(aggregated_BT_chunks, 'aggregated_BT_chunks.pkl')

    print('Creating bt_level_meme_names..', flush = True)
    perc_BT = aggregated_BT_chunks['%BT']
    noun_chunks = aggregated_BT_chunks['noun_chunks']
    counter_noun_chunks = []
    noun_chunk_name = []
    count = []
    bt_memes = []
    counter_memes = []

    print('Creating a noun chunk counter..', flush = True)
    for i in range(len(noun_chunks)):
        counter_noun_chunks.append(Counter(aggregated_BT_chunks['noun_chunks'][i]).most_common())
        cnt = []
        for j in range(len(counter_noun_chunks[i])):
            cnt.append(counter_noun_chunks[i][j][1])
        count.append(cnt)

    print('Matching ', len(noun_chunks),' noun chunks with ', len(chunk_meme_name['meme_name']), ' memes..', flush = True)        
    for i in range(len(noun_chunks)):
        memes = []
        print('I: ', i, flush = True)
        for j in range(len(counter_noun_chunks[i])):
            if (j%1000 == 0):
                print('J: ', j, flush = True)
            for k in range(len(chunk_meme_name)):
                if counter_noun_chunks[i][j] == chunk_meme_name['noun_chunk'][k]:
                    memes.append(chunk_meme_name['meme_name'][k])
                    break
        bt_memes.append(memes)

    def merge(list1, list2):  
        merged_list = [(list1[i], list2[i]) for i in range(0, len(list1))]
        return merged_list

    print('Creating a meme counter..', flush = True)
    for i in range(len(noun_chunks)):
        pairs_meme_count = merge(bt_memes[i], count[i])
        counter_memes.append(Counter(dict(pairs_meme_count)).most_common())

    print('Saving bt_level_meme_names into pkl and parquet..', flush = True)
    bt_level_meme_names = pd.DataFrame({'%BT': perc_BT, 'noun_chunks': noun_chunks, 
                                        'counter_noun_chunks': counter_noun_chunks, 'noun_chunk_name': noun_chunk_name, 
                                        'count': count, 'meme_names': bt_memes, 'counter_memes': counter_memes})

    save_object(bt_level_meme_names, 'bt_level_meme_names.pkl')
    bt_level_meme_names.to_parquet('bt_level_meme_names.parquet')

    print('Creating top_20_memes_for_BT_level..', flush = True)
    top_20_meme_names = []
    top_20_meme_occurences = []
    for i in range(len(bt_level_meme_names)):
        meme_20 = []
        occ_20 = []
        for i in range(20):
            meme_20.append(bt_level_meme_names['counter_memes'][i][:20][i][0])
            occ_20.append(bt_level_meme_names['counter_memes'][i][:20][i][1])
        top_20_meme_names.append(meme_20)
        top_20_meme_occurences.append(occ_20)

    top_20_memes_for_BT_level = pd.DataFrame({'%BT': perc_BT, 'top_20_meme_names': top_20_meme_names, 'top_20_meme_occurences': top_20_meme_occurences})

    print('Saving top_20_memes_for_BT_level..', flush = True)
    save_object(top_20_memes_for_BT_level, 'top_20_memes_for_BT_level.pkl')
    top_20_memes_for_BT_level.to_parquet('top_20_memes_for_BT_level.parquet')

if __name__=="__main__":
    typer.run(main)
