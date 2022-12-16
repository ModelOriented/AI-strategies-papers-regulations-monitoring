import pandas as pd
import numpy as np
import pickle
from collections import Counter
import typer
import os
from itertools import islice

def main(cluster_name : str, chunk_meme : str, processed_big : str, bt_perc: str, output_dir: str):
    '''
    cluster_name : cluster_names/reduced_300_big_bleaned_phrase-bert_eps_0.2_min_clust_size_3
    chunk_meme : chunk_meme_mappings/reduced_300_big_bleaned_phrase-bert_eps_0.2_min_clust_size_3
    processed_big : processed_big
    bt_perc : big_ai_dataset_with_affiliations_nb
    output_dir : data/s2orc
    '''
    print('Changing to output dir...', flush = True)

    os.chdir(output_dir)

    checkpoint_1 = False
    checkpoint_2 = False
    checkpoint_3 = False
    checkpoint_4 = False
    checkpoint_5 = False
    checkpoint_6 = False


    def save_object(obj, filename):
        with open(filename, 'wb') as outp:  # Overwrites any existing file.
            pickle.dump(obj, outp, pickle.HIGHEST_PROTOCOL)
    
    def take(n, iterable):
        return list(islice(iterable, n))
    
    
    def merge(list1, list2):  
        merged_list = [(list1[i], list2[i]) for i in range(0, len(list1))]
        return merged_list
    
    for file_name in os.listdir(output_dir):
        if file_name == 'bt_perc_reduced.parquet' :
            checkpoint_1 = True
        if file_name == 'chunk_meme_name.parquet' :
            checkpoint_4 = True
        if file_name == 'aggregated_BT_chunks.pkl' :
            checkpoint_2 = True
        if file_name == 'bt_level_meme_names.pkl':
            checkpoint_6 = True
        if file_name == 'noun_chunk_counter.pkl' :
            checkpoint_3 = True
        if file_name == 'chunk_meme_dict.pkl' :
            checkpoint_5 = True
    
    print('Reading the files..', flush = True)
    if checkpoint_4 == False:
        cluster_name  = pd.read_parquet(cluster_name)
        chunk_meme    = pd.read_parquet(chunk_meme)
    if checkpoint_2 == False:  
        processed_big = pd.read_parquet(processed_big)
    if checkpoint_1 == False:
        bt_perc       = pd.read_parquet(bt_perc)
    
    print('Sample dataset observations (if checkpoints 1,2,4 are passed, it gives no output)..', flush = True)
    if checkpoint_4 == False:
        print(cluster_name.head(), flush = True)
        print(chunk_meme.head(), flush = True)
    if checkpoint_2 == False: 
        print(processed_big.head(), flush = True)
    if checkpoint_1 == False:
        print(bt_perc.head(), flush = True)
    


    print('CHECKPOINT 1', flush = True)
    if checkpoint_1 == True :
        print('Data ready, loading the dataframe..', flush = True)
        bt_perc_reduced = pd.read_parquet('bt_perc_reduced.parquet')

    else :
        print('Data not ready..', flush = True)
        print('Reducing information about papers (bt_perc)..', flush = True)
        bt_perc_reduced = bt_perc[['paper_id', '%BT']].reset_index()

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

    print('bt_perc_reduced', flush = True)
    print(bt_perc_reduced.head(), flush = True)



    print('CHECKPOINT 2', flush = True)
    if checkpoint_2 == True :
        print('Data ready, loading the dataframe..', flush = True)
        aggregated_BT_chunks = pd.read_pickle('aggregated_BT_chunks.pkl')

    else :
        print('Data not ready..', flush = True)
        print('Reducing information from processed_big..', flush = True)
        processed_reduced = processed_big[['paper_id', 'noun_chunks_cleaned']]

        print('Merging bt_perc_reduced and processed_reduced into paper_bt_chunks..', flush = True)
        paper_bt_chunks         = pd.merge(bt_perc_reduced, processed_reduced, on = 'paper_id', how = 'inner')[['paper_id', '%BT','noun_chunks_cleaned']]
        paper_bt_chunks.columns = ['paper_id', '%BT', 'noun_chunks']

        if len(pd.unique(paper_bt_chunks['paper_id'])) != len(paper_bt_chunks['paper_id']) :
            print('Duplicate papers appear in paper_bt_chunks', flush = True)

        print('Creating aggregated_BT_chunks from paper_bt_chunks..', flush = True)
        perc_BT = [0, 0.05, 0.1, 0.15, 0.2, 0.25,  0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1]
        x = []
        agg_chunks = [np.asarray(x) for i in range(21)]

        for i in range(len(paper_bt_chunks)):
            for j in np.arange(0, 1.05, 0.05):
                if paper_bt_chunks['%BT'][i] == round(j,3):
                    # unique so that we have 1 noun chunk representant in the paper
                    agg_chunks[int(j*20)] = np.concatenate((agg_chunks[int(j*20)], pd.unique(paper_bt_chunks['noun_chunks'][i])))
                    break

        aggregated_BT_chunks = pd.DataFrame({'%BT': perc_BT, 'noun_chunks': agg_chunks})
        # to_parquet didnt work

        print('Saving aggregated_BT_chunks into pkl..', flush = True)
        save_object(aggregated_BT_chunks, 'aggregated_BT_chunks.pkl')

    print('aggregated_BT_chunks', flush = True)
    print(aggregated_BT_chunks.head(), flush = True)

    print('Creating bt_level_meme_names..', flush = True)
    perc_BT             = aggregated_BT_chunks['%BT']
    noun_chunks         = aggregated_BT_chunks['noun_chunks']
    counter_noun_chunks = []
    noun_chunk_name     = []
    count               = []
    bt_memes            = []
    counter_memes       = []

    
    
    print('CHECKPOINT 3', flush = True)
    if checkpoint_3 == True :
        print('Data ready, loading the object..', flush = True)
        count               = pd.read_pickle('noun_chunk_counter.pkl')
        counter_noun_chunks = pd.read_pickle('counter_noun_chunks.pkl')
        noun_chunk_name     = pd.read_pickle('noun_chunk_name.pkl')

    else :
        print('Data not ready..', flush = True)
        print('Creating a noun chunk counter..', flush = True)

        for i in range(len(noun_chunks)):
            counter_noun_chunks.append(Counter(aggregated_BT_chunks['noun_chunks'][i]).most_common())
            cnt = []
            nm  = []
            for j in range(len(counter_noun_chunks[i])):
                cnt.append(counter_noun_chunks[i][j][1])
                nm.append(counter_noun_chunks[i][j][0])
            count.append(cnt)
            noun_chunk_name.append(nm)

        save_object(count, 'noun_chunk_counter.pkl')
        save_object(counter_noun_chunks, 'counter_noun_chunks.pkl')
        save_object(noun_chunk_name, 'noun_chunk_name.pkl')
    
    print('counter_noun_chunks', flush = True)
    print(counter_noun_chunks[0][:10], flush = True)
    
       

    print('CHECKPOINT 4', flush = True)
    if checkpoint_4 == True :
        print('Data ready, loading the object..', flush = True)
        chunk_meme_name = pd.read_parquet('chunk_meme_name.parquet')
    else :
        print('Data not ready..', flush = True)
        print('Merging chunk_meme and cluster_name..', flush = True)

        chunk_meme_name         = pd.merge(chunk_meme, cluster_name, on = 'meme_id', how = 'inner') [['chunk', 'most_common']]
        chunk_meme_name.columns = ['noun_chunk', 'meme_name']
        chunk_meme_name.to_parquet('chunk_meme_name.parquet')

    print('chunk_meme_name', flush = True)
    print(chunk_meme_name, flush = True)




    print('CHECKPOINT 5', flush = True)
    if checkpoint_5 == True :
        print('Data ready, loading the object..', flush = True)
        chunk_meme_dict = pd.read_pickle('chunk_meme_dict.pkl')

    else :
        print('Data not ready..', flush = True)
        print('Creating a dictionary..', flush = True)

        chunk_meme_dict = dict(zip(chunk_meme_name.noun_chunk, chunk_meme_name.meme_name))
        save_object(chunk_meme_dict, 'chunk_meme_dict.pkl')

    print('chunk_meme_dict', flush = True)
    print(take(10, chunk_meme_dict.items()), flush = True)





    print('CHECKPOINT 6', flush = True)
    if checkpoint_6 == True :
        print('Data ready, loading the object..', flush = True)
        bt_level_meme_names = pd.read_pickle('bt_level_meme_names.pkl')

    else :
        print('Data not ready..', flush = True)
        print('Matching ', len(noun_chunks[0]),' noun chunks with ', len(chunk_meme_name['meme_name']), ' memes..', flush = True)
        for i in range(len(noun_chunks)) :
            memes = []
            for j in range(len(counter_noun_chunks[i])):
                memes.append(chunk_meme_dict[counter_noun_chunks[i][j][0]])
            bt_memes.append(memes)

        print('Creating merged meme counts..', flush = True)
        merged_bt_memes = []
        merged_counts   = []

        for i in range(len(bt_memes)):
            merged_meme_counts = pd.DataFrame({'bt_memes': bt_memes[i], 'counter': count[i]})
            dff = merged_meme_counts.groupby('bt_memes').counter.sum().reset_index()
            merged_bt_memes.append(list(dff['bt_memes']))
            merged_counts.append(list(dff['counter']))

        print('Creating a meme counter..', flush = True)

        for i in range(len(noun_chunks)):
            pairs_meme_count = merge(merged_bt_memes[i], merged_counts[i])
            counter_memes.append(Counter(dict(pairs_meme_count)).most_common())

        merged_bt_memes = list(merged_bt_memes)
        merged_counts   = list(merged_counts)

        print(counter_memes[0][:10])
        print('Saving bt_level_meme_names into pkl..', flush = True) # because of counter
        bt_level_meme_names = pd.DataFrame({'%BT': perc_BT, 'noun_chunks': noun_chunks, 
                                            'counter_noun_chunks': counter_noun_chunks, 'noun_chunk_name': noun_chunk_name, 
                                            'count': count, 'meme_names': bt_memes, 'merged_bt_memes': merged_bt_memes, 'merged_counts': merged_counts, 'counter_memes': counter_memes})                              

        save_object(bt_level_meme_names, 'bt_level_meme_names.pkl')
    
    print('bt_level_meme_names', flush = True)
    print(bt_level_meme_names.head(), flush = True)



    print('CHECKPOINT 7', flush = True)
    print('Creating top_20_memes_for_BT_level..', flush = True)
    top_20_meme_names      = []
    top_20_meme_occurences = []
    for i in range(len(bt_level_meme_names)):
        meme_20 = []
        occ_20  = []
        for j in range(min(20, len(bt_level_meme_names['counter_memes'][i][:20]))):
            print((i,j))
            meme_20.append(bt_level_meme_names['counter_memes'][i][:20][j][0])
            occ_20.append(bt_level_meme_names['counter_memes'][i][:20][j][1])
        top_20_meme_names.append(meme_20)
        top_20_meme_occurences.append(occ_20)

    top_20_memes_for_BT_level = pd.DataFrame({'%BT': perc_BT, 'top_20_meme_names': top_20_meme_names, 'top_20_meme_occurences': top_20_meme_occurences})
    
    print('Saving top_20_memes_for_BT_level..', flush = True)
    top_20_memes_for_BT_level.to_parquet('top_20_memes_for_BT_level.parquet')
    top_20_memes_for_BT_level.to_csv('top_20_memes_for_BT_level.csv')

    print('top_20_memes_for_BT_level', flush = True)
    print(top_20_memes_for_BT_level, flush = True)

    for i in range(21):
        print(top_20_memes_for_BT_level['top_20_meme_names'][i], flush = True)


if __name__=="__main__":
    typer.run(main)