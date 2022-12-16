import pandas as pd
import numpy as np
import pickle
from collections import Counter
import typer
import os
from itertools import islice
import copy

def main(cluster_name : str, chunk_meme : str, processed_big : str, company_perc: str, output_dir: str):
    '''
    cluster_name : cluster_names/reduced_300_big_bleaned_phrase-bert_eps_0.2_min_clust_size_3
    chunk_meme : chunk_meme_mappings/reduced_300_big_bleaned_phrase-bert_eps_0.2_min_clust_size_3
    processed_big : processed_big
    company_perc : big_ai_dataset_with_affiliations_nb
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
    checkpoint_7 = False


    def save_object(obj, filename):
        with open(filename, 'wb') as outp:  # Overwrites any existing file.
            pickle.dump(obj, outp, pickle.HIGHEST_PROTOCOL)
    
    def take(n, iterable):
        return list(islice(iterable, n))
    
    
    def merge(list1, list2):  
        merged_list = [(list1[i], list2[i]) for i in range(0, len(list1))]
        return merged_list
    
    for file_name in os.listdir(output_dir):
        if file_name == 'company_perc_reduced.parquet' :
            checkpoint_1 = True
        if file_name == 'chunk_meme_name.parquet' :
            checkpoint_4 = True
        if file_name == 'paper_company_chunks.pkl' :
            checkpoint_2 = True
        if file_name == 'paper_company_memes.pkl':
            checkpoint_6 = True
        if file_name == 'noun_chunk_counter.pkl' :
            checkpoint_3 = True
        if file_name == 'chunk_meme_dict.pkl' :
            checkpoint_5 = True
        if file_name == 'agg_company_memes.pkl':
            checkpoint_7 = True
    
    print('Reading the files..', flush = True)
    if checkpoint_4 == False:
        cluster_name  = pd.read_parquet(cluster_name)
        chunk_meme    = pd.read_parquet(chunk_meme)
    if checkpoint_2 == False:  
        processed_big = pd.read_parquet(processed_big)
    if checkpoint_1 == False:
        company_perc  = pd.read_parquet(company_perc)
    
    print('Sample dataset observations (if checkpoints 1,2,4 are passed, it gives no output)..', flush = True)
    if checkpoint_4 == False:
        print(cluster_name.head(), flush = True)
        print(chunk_meme.head(), flush = True)
    if checkpoint_2 == False: 
        print(processed_big.head(), flush = True)
    if checkpoint_1 == False:
        print(company_perc.head(), flush = True)
    


    print('CHECKPOINT 1', flush = True)
    if checkpoint_1 == True :
        print('Data ready, loading the dataframe..', flush = True)
        company_perc_reduced = pd.read_parquet('company_perc_reduced.parquet')

    else :
        print('Data not ready..', flush = True)
        print('Reducing information about papers (company_perc)..', flush = True)
        #bt_perc_reduced = bt_perc[['paper_id', '%BT']].reset_index()
        company_perc_reduced = company_perc[['paper_id', 'company']].reset_index()

        print('Binning company percantage..', flush = True)
        for i in range(len(company_perc_reduced)):
            #bt = bt_perc_reduced['%BT'][i]
            bt = company_perc_reduced['company'][i]
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
            company_perc_reduced['company'][i] = bt
        
        print('Saving reduced dataframe..', flush = True)
        company_perc_reduced.to_parquet('company_perc_reduced.parquet')

    print('company_perc_reduced', flush = True)
    print(company_perc_reduced.head(), flush = True)
    print('length :',len(company_perc_reduced), flush = True)


    print('CHECKPOINT 2', flush = True)
    if checkpoint_2 == True :
        print('Data ready, loading the dataframe..', flush = True)
        paper_company_chunks = pd.read_pickle('paper_company_chunks.pkl')

    else :
        print('Data not ready..', flush = True)
        print('Reducing information from processed_big..', flush = True)
        processed_reduced = processed_big[['paper_id', 'noun_chunks_cleaned']]

        print('Merging company_perc_reduced and processed_reduced into paper_company_chunks..', flush = True)
        paper_company_chunks         = pd.merge(company_perc_reduced, processed_reduced, on = 'paper_id', how = 'inner')[['paper_id', 'company','noun_chunks_cleaned']]
        paper_company_chunks.columns = ['paper_id', 'company%', 'noun_chunks']

        print('Saving paper_company_chunks into pkl..', flush = True)
        save_object(paper_company_chunks, 'paper_company_chunks.pkl')

    print('paper_company_chunks', flush = True)
    print(paper_company_chunks.head(), flush = True)
    print('length :',len(paper_company_chunks), flush = True)

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
    print('length :',len(chunk_meme_name), flush = True)



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
    print('length :',len(chunk_meme_dict), flush = True)




    print('CHECKPOINT 6', flush = True)
    paper_memes = []
    if checkpoint_6 == True :
        print('Data ready, loading the object..', flush = True)
        paper_company_memes = pd.read_pickle('paper_company_memes.pkl')

    else :
        print('Data not ready..', flush = True)
        for i in range(len(paper_company_chunks)):
            memes = []
            for j in range(len(paper_company_chunks['noun_chunks'][i])):
                memes.append(chunk_meme_dict[paper_company_chunks['noun_chunks'][i][j]])
            paper_memes.append(pd.unique(memes))

        paper_company_memes = pd.DataFrame({'paper_id': paper_company_chunks['paper_id'], 'company%': paper_company_chunks['company%'], 'unique_memes': paper_memes})
        save_object(paper_company_memes, 'paper_company_memes.pkl')

    print('paper_company_memes', flush = True)
    print(paper_company_memes.head(), flush = True)
    print('length :',len(paper_company_memes), flush = True)




    print('CHECKPOINT 7', flush = True)
    perc_company = [0, 0.05, 0.1, 0.15, 0.2, 0.25,  0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1]

    if checkpoint_7 == True :
        print('Data ready, loading the object..', flush = True)
        agg_company_memes = pd.read_pickle('agg_company_memes.pkl')

    else :
        print('Creating aggregated memes from paper_company_memes..', flush = True)
        x = []
        nr_of_papers = [0 for i in range(21)]
        agg_memes = [np.asarray(x) for i in range(21)]
        
        for i in range(len(paper_company_memes)):
            for j in np.arange(0, 1.05, 0.05):
                if paper_company_memes['company%'][i] == round(j,3):
                    # unique so that we have 1 meme representant in the paper
                    nr_of_papers[int(j*20)] += 1
                    agg_memes[int(j*20)] = np.concatenate((agg_memes[int(j*20)], pd.unique(paper_company_memes['unique_memes'][i])))
                    break
        
        print('Creating meme counter..', flush = True)
        counter_memes = []
        count = []
        meme_name = []
        for i in range(len(agg_memes)):
            counter_memes.append(Counter(agg_memes[i]).most_common())
            cnt = []
            nm  = []
            for j in range(len(counter_memes[i])):
                cnt.append(counter_memes[i][j][1])
                nm.append(counter_memes[i][j][0])
            count.append(cnt)
            meme_name.append(nm)
        
        meme_perc = []
        for i in range (len(count)):
            meme_per = []
            for j in range(len(count[i])):
                meme_per.append(count[i][j] / nr_of_papers[i])
            meme_perc.append(meme_per)

        agg_company_memes = pd.DataFrame({'company%': perc_company, 'memes_counter': counter_memes, 'memes': meme_name, 'counts': count, 'nr_of_papers' : nr_of_papers, 'meme_perc' : meme_perc})
        save_object(agg_company_memes, 'agg_company_memes.pkl')


    print('agg_company_memes', flush = True)
    print(agg_company_memes.head(), flush = True)
    print('length :',len(agg_company_memes), flush = True)





    print('CHECKPOINT 8', flush = True)
    print('Creating top_20_memes_for_company_level..', flush = True)
    top_20_meme_names      = []
    top_20_meme_occurences = []
    top_20_meme_perc       = []
    for i in range(len(agg_company_memes)):
        meme_20 = []
        occ_20  = []
        perc_20 = []
        for j in range(min(20, len(agg_company_memes['memes_counter'][i][:20]))):
            meme_20.append(agg_company_memes['memes_counter'][i][:20][j][0])
            occ_20.append(agg_company_memes['memes_counter'][i][:20][j][1])
            perc_20.append(agg_company_memes['meme_perc'][i][:20][j])
        top_20_meme_names.append(meme_20)
        top_20_meme_occurences.append(occ_20)
        top_20_meme_perc.append(perc_20)

    top_20_memes_for_company_level = pd.DataFrame({'company%': perc_company, 'top_20_meme_names': top_20_meme_names, 'top_20_meme_occurences': top_20_meme_occurences, 'top_20_meme_perc': top_20_meme_perc})
    
    print('Saving top_20_memes_for_company_level..', flush = True)
    top_20_memes_for_company_level.to_parquet('top_20_memes_for_company_level.parquet')
    top_20_memes_for_company_level.to_csv('top_20_memes_for_company_level.csv')

    print('top_20_memes_for_company_level', flush = True)
    print(top_20_memes_for_company_level, flush = True)





    print('CHECKPOINT 9', flush = True)
    print('Creating word-freq-word-freq table', flush = True)

    perc_company = [0, 0.05, 0.1, 0.15, 0.2, 0.25,  0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1]

    min_idx = 30
    for i in range(len(top_20_memes_for_company_level)):
        if (len(top_20_memes_for_company_level['top_20_meme_names'][i]) < min_idx):
            min_idx = len(top_20_memes_for_company_level['top_20_meme_names'][i])


    meme_1 = top_20_memes_for_company_level['top_20_meme_names'][0][:min_idx]
    meme_2 = top_20_memes_for_company_level['top_20_meme_names'][1][:min_idx]
    meme_3 = top_20_memes_for_company_level['top_20_meme_names'][2][:min_idx]
    meme_4 = top_20_memes_for_company_level['top_20_meme_names'][3][:min_idx]
    meme_5 = top_20_memes_for_company_level['top_20_meme_names'][4][:min_idx]
    meme_6 = top_20_memes_for_company_level['top_20_meme_names'][5][:min_idx]
    meme_7 = top_20_memes_for_company_level['top_20_meme_names'][6][:min_idx]
    meme_8 = top_20_memes_for_company_level['top_20_meme_names'][7][:min_idx]
    meme_9 = top_20_memes_for_company_level['top_20_meme_names'][8][:min_idx]
    meme_10 = top_20_memes_for_company_level['top_20_meme_names'][9][:min_idx]
    meme_11 = top_20_memes_for_company_level['top_20_meme_names'][10][:min_idx]
    meme_12 = top_20_memes_for_company_level['top_20_meme_names'][11][:min_idx]
    meme_13 = top_20_memes_for_company_level['top_20_meme_names'][12][:min_idx]
    meme_14 = top_20_memes_for_company_level['top_20_meme_names'][13][:min_idx]
    meme_15 = top_20_memes_for_company_level['top_20_meme_names'][14][:min_idx]
    meme_16 = top_20_memes_for_company_level['top_20_meme_names'][15][:min_idx]
    meme_17 = top_20_memes_for_company_level['top_20_meme_names'][16][:min_idx]
    meme_18 = top_20_memes_for_company_level['top_20_meme_names'][17][:min_idx]
    meme_19 = top_20_memes_for_company_level['top_20_meme_names'][18][:min_idx]
    meme_20 = top_20_memes_for_company_level['top_20_meme_names'][19][:min_idx]
    meme_21 = top_20_memes_for_company_level['top_20_meme_names'][20][:min_idx]

    perc_1 = top_20_memes_for_company_level['top_20_meme_perc'][0][:min_idx]
    perc_2 = top_20_memes_for_company_level['top_20_meme_perc'][1][:min_idx]
    perc_3 = top_20_memes_for_company_level['top_20_meme_perc'][2][:min_idx]
    perc_4 = top_20_memes_for_company_level['top_20_meme_perc'][3][:min_idx]
    perc_5 = top_20_memes_for_company_level['top_20_meme_perc'][4][:min_idx]
    perc_6 = top_20_memes_for_company_level['top_20_meme_perc'][5][:min_idx]
    perc_7 = top_20_memes_for_company_level['top_20_meme_perc'][6][:min_idx]
    perc_8 = top_20_memes_for_company_level['top_20_meme_perc'][7][:min_idx]
    perc_9 = top_20_memes_for_company_level['top_20_meme_perc'][8][:min_idx]
    perc_10 = top_20_memes_for_company_level['top_20_meme_perc'][9][:min_idx]
    perc_11 = top_20_memes_for_company_level['top_20_meme_perc'][10][:min_idx]
    perc_12 = top_20_memes_for_company_level['top_20_meme_perc'][11][:min_idx]
    perc_13 = top_20_memes_for_company_level['top_20_meme_perc'][12][:min_idx]
    perc_14 = top_20_memes_for_company_level['top_20_meme_perc'][13][:min_idx]
    perc_15 = top_20_memes_for_company_level['top_20_meme_perc'][14][:min_idx]
    perc_16 = top_20_memes_for_company_level['top_20_meme_perc'][15][:min_idx]
    perc_17 = top_20_memes_for_company_level['top_20_meme_perc'][16][:min_idx]
    perc_18 = top_20_memes_for_company_level['top_20_meme_perc'][17][:min_idx]
    perc_19 = top_20_memes_for_company_level['top_20_meme_perc'][18][:min_idx]
    perc_20 = top_20_memes_for_company_level['top_20_meme_perc'][19][:min_idx]
    perc_21 = top_20_memes_for_company_level['top_20_meme_perc'][20][:min_idx]

    meme_perc_table = pd.DataFrame({ 
    'meme_1': meme_1, 'perc_1': perc_1, 
    'meme_2': meme_2, 'perc_2': perc_2, 
    'meme_3': meme_3, 'perc_3': perc_3, 
    'meme_4': meme_4, 'perc_4': perc_4, 
    'meme_5': meme_5, 'perc_5': perc_5, 
    'meme_6': meme_6, 'perc_6': perc_6, 
    'meme_7': meme_7, 'perc_7': perc_7, 
    'meme_8': meme_8, 'perc_8': perc_8, 
    'meme_9': meme_9, 'perc_9': perc_9, 
    'meme_10': meme_10, 'perc_10': perc_10, 
    'meme_11': meme_11, 'perc_11': perc_11, 
    'meme_12': meme_12, 'perc_12': perc_12, 
    'meme_13': meme_13, 'perc_13': perc_13, 
    'meme_14': meme_14, 'perc_14': perc_14, 
    'meme_15': meme_15, 'perc_15': perc_15, 
    'meme_16': meme_16, 'perc_16': perc_16, 
    'meme_17': meme_17, 'perc_17': perc_17, 
    'meme_18': meme_18, 'perc_18': perc_18, 
    'meme_19': meme_19, 'perc_19': perc_19, 
    'meme_20': meme_20, 'perc_20': perc_20,
    'meme_21': meme_21, 'perc_21': perc_21})

    print('meme_perc_table', flush = True)
    print(meme_perc_table, flush = True)

    print('Saving meme_perc_table..', flush = True)
    meme_perc_table.to_parquet('meme_perc_table.parquet')
    meme_perc_table.to_csv('meme_perc_table.csv')



    print('CHECKPOINT 10', flush = True)
    agg_company_memes  = pd.read_pickle('agg_company_memes.pkl')
    meme_counters = agg_company_memes['memes_counter']

    academia_to_company = copy.deepcopy(meme_counters)
    company_to_academia = copy.deepcopy(meme_counters)

    academia_to_company_nr_of_papers = copy.deepcopy(agg_company_memes['nr_of_papers'])
    company_to_academia_nr_of_papers = copy.deepcopy(agg_company_memes['nr_of_papers'])

    academia_to_company_dicts = [[] for i in range(len(meme_counters))]
    company_to_academia_dicts = [[] for i in range(len(meme_counters))]

    # academia_to_bt_dicts
    for i in range(18, 0, -1):
        academia_to_company[i] = academia_to_company[i] + academia_to_company[i + 1]
        academia_to_company_nr_of_papers[i] = academia_to_company_nr_of_papers[i] + academia_to_company_nr_of_papers[i + 1]

    for i in range(len(meme_counters)):
        dic = dict()
        x = academia_to_company[i]
        for j in x: 
            if j[0] not in dic: 
                dic[j[0]] = j[1] 
            else: 
                dic[j[0]] += j[1]
        dic = dict(sorted(dic.items(), key=lambda item: item[1], reverse=True))
        academia_to_company_dicts[i] = dic.copy()

    # bt_to_academia_dicts
    for i in range(2, 20):
        company_to_academia[i] = company_to_academia[i] + company_to_academia[i - 1]
        company_to_academia_nr_of_papers[i] = company_to_academia_nr_of_papers[i] + company_to_academia_nr_of_papers[i - 1]
    print("")
    for i in range(len(meme_counters)):
        dic = dict()
        x = company_to_academia[i]
        for j in x: 
            if j[0] not in dic: 
                dic[j[0]] = j[1] 
            else: 
                dic[j[0]] += j[1]
        dic = dict(sorted(dic.items(), key=lambda item: item[1], reverse=True))
        company_to_academia_dicts[i] = dic.copy()

    # academia_to_bt_cumulative_memes

    academia_to_company_count     = []
    academia_to_company_meme_name = []

    for i in range(len(meme_counters)):
        academia_to_company_memes = list(academia_to_company_dicts[i].items())
        cnt_a_company = []
        nm_a_company  = []

        for j in range(len(academia_to_company_memes)):
            cnt_a_company.append(academia_to_company_memes[j][1])
            nm_a_company.append(academia_to_company_memes[j][0])
        academia_to_company_count.append(cnt_a_company)
        academia_to_company_meme_name.append(nm_a_company)
            
        academia_to_company_meme_perc = []
        for i in range (len(academia_to_company_count)):
            academia_to_company_meme_per = []
            for j in range(len(academia_to_company_count[i])):
                academia_to_company_meme_per.append(academia_to_company_count[i][j] / academia_to_company_nr_of_papers[i])
            academia_to_company_meme_perc.append(academia_to_company_meme_per)

    perc_company = [0, 0.05, 0.1, 0.15, 0.2, 0.25,  0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1]
    academia_to_company_cumulative_memes = pd.DataFrame({'company%': perc_company, 'memes_counter': academia_to_company_dicts, 'memes': academia_to_company_meme_name, 'counts': academia_to_company_count, 'nr_of_papers' : academia_to_company_nr_of_papers, 'meme_perc' : academia_to_company_meme_perc})

    print('academia_to_company_cumulative_memes', flush = True) 
    print(academia_to_company_cumulative_memes)

    # bt_to_academia_cumulative_memes

    company_to_academia_count     = []
    company_to_academia_meme_name = []

    for i in range(len(meme_counters)):
        company_to_academia_memes = list(company_to_academia_dicts[i].items())
        cnt_a_company = []
        nm_a_company  = []

        for j in range(len(company_to_academia_memes)):
            cnt_a_company.append(company_to_academia_memes[j][1])
            nm_a_company.append(company_to_academia_memes[j][0])
        company_to_academia_count.append(cnt_a_company)
        company_to_academia_meme_name.append(nm_a_company)
            
        company_to_academia_meme_perc = []
        for i in range (len(company_to_academia_count)):
            company_to_academia_meme_per = []
            for j in range(len(company_to_academia_count[i])):
                company_to_academia_meme_per.append(company_to_academia_count[i][j] / company_to_academia_nr_of_papers[i])
            company_to_academia_meme_perc.append(company_to_academia_meme_per)

    company_to_academia_cumulative_memes = pd.DataFrame({'company%': perc_company, 'memes_counter': company_to_academia_dicts, 'memes': company_to_academia_meme_name, 'counts': company_to_academia_count, 'nr_of_papers' : company_to_academia_nr_of_papers, 'meme_perc' : company_to_academia_meme_perc})

    print('company_to_academia_cumulative_memes', flush = True) 
    print(company_to_academia_cumulative_memes, flush = True) 

    print('Saving academia_to_company_cumulative_memes and company_to_academia_cumulative_memes..', flush = True)

    academia_to_company_cumulative_memes.to_parquet('academia_to_company_cumulative_memes.parquet')
    academia_to_company_cumulative_memes.to_csv('academia_to_company_cumulative_memes.csv')

    company_to_academia_cumulative_memes.to_parquet('company_to_academia_cumulative_memes.parquet')
    company_to_academia_cumulative_memes.to_csv('company_to_academia_cumulative_memes.csv')

    print('CHECKPOINT 11', flush = True)
    print('Creating top_20_memes_for_cumulative_company_level..', flush = True)
    academia_to_company_top_20_meme_names      = []
    academia_to_company_top_20_meme_occurences = []
    academia_to_company_top_20_meme_perc       = []
    company_to_academia_top_20_meme_names      = []
    company_to_academia_top_20_meme_occurences = []
    company_to_academia_top_20_meme_perc       = []
    for i in range(len(academia_to_company_cumulative_memes)):
        meme_20 = []
        occ_20  = []
        perc_20 = []
        #print(academia_to_company_cumulative_memes['memes_counter'][i])
        #print(academia_to_company_cumulative_memes['memes_counter'][i][:20])
        #print(len(academia_to_company_cumulative_memes['memes_counter'][i][:20]))
        for j in range(min(20, len(academia_to_company_cumulative_memes['memes'][i][:20]))):
            meme_20.append(academia_to_company_cumulative_memes['memes'][i][:20][j])
            occ_20.append(academia_to_company_cumulative_memes['counts'][i][:20][j])
            perc_20.append(academia_to_company_cumulative_memes['meme_perc'][i][:20][j])
        academia_to_company_top_20_meme_names.append(meme_20)
        academia_to_company_top_20_meme_occurences.append(occ_20)
        academia_to_company_top_20_meme_perc.append(perc_20)

    academia_to_company_top_20_memes_for_company_level = pd.DataFrame({'company%': perc_company, 'top_20_meme_names': academia_to_company_top_20_meme_names, 'top_20_meme_occurences': academia_to_company_top_20_meme_occurences, 'top_20_meme_perc': academia_to_company_top_20_meme_perc})
    
    print('Saving academia_to_company_top_20_memes_for_BT_level..', flush = True)
    academia_to_company_top_20_memes_for_company_level.to_parquet('academia_to_company_top_20_memes_for_BT_level.parquet')
    academia_to_company_top_20_memes_for_company_level.to_csv('academia_to_company_top_20_memes_for_BT_level.csv')

    print('academia_to_company_top_20_memes_for_company_level', flush = True)
    print(academia_to_company_top_20_memes_for_company_level, flush = True)

    for i in range(len(company_to_academia_cumulative_memes)):
        meme_20 = []
        occ_20  = []
        perc_20 = []
        for j in range(min(20, len(company_to_academia_cumulative_memes['memes'][i][:20]))):
            meme_20.append(company_to_academia_cumulative_memes['memes'][i][:20][j])
            occ_20.append(company_to_academia_cumulative_memes['counts'][i][:20][j])
            perc_20.append(company_to_academia_cumulative_memes['meme_perc'][i][:20][j])
        company_to_academia_top_20_meme_names.append(meme_20)
        company_to_academia_top_20_meme_occurences.append(occ_20)
        company_to_academia_top_20_meme_perc.append(perc_20)

    company_to_academia_top_20_memes_for_company_level = pd.DataFrame({'company%': perc_company, 'top_20_meme_names': company_to_academia_top_20_meme_names, 'top_20_meme_occurences': company_to_academia_top_20_meme_occurences, 'top_20_meme_perc': company_to_academia_top_20_meme_perc})
    
    print('Saving academia_to_comapny_top_20_memes_for_company_level..', flush = True)
    company_to_academia_top_20_memes_for_company_level.to_parquet('company_to_academia_top_20_memes_for_company_level.parquet')
    company_to_academia_top_20_memes_for_company_level.to_csv('company_to_academia_top_20_memes_for_company_level.csv')

    print('company_to_academia_top_20_memes_for_company_level', flush = True)
    print(company_to_academia_top_20_memes_for_company_level, flush = True)





    print('CHECKPOINT 12', flush = True)
    print('Creating word-freq-word-freq cumulative tables', flush = True)

    perc_company = [0, 0.05, 0.1, 0.15, 0.2, 0.25,  0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1]

    min_idx = 30
    for i in range(len(academia_to_company_top_20_memes_for_company_level)):
        if (len(academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][i]) < min_idx):
            min_idx = len(academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][i])


    meme_1 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][0][:min_idx]
    meme_2 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][1][:min_idx]
    meme_3 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][2][:min_idx]
    meme_4 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][3][:min_idx]
    meme_5 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][4][:min_idx]
    meme_6 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][5][:min_idx]
    meme_7 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][6][:min_idx]
    meme_8 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][7][:min_idx]
    meme_9 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][8][:min_idx]
    meme_10 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][9][:min_idx]
    meme_11 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][10][:min_idx]
    meme_12 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][11][:min_idx]
    meme_13 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][12][:min_idx]
    meme_14 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][13][:min_idx]
    meme_15 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][14][:min_idx]
    meme_16 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][15][:min_idx]
    meme_17 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][16][:min_idx]
    meme_18 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][17][:min_idx]
    meme_19 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][18][:min_idx]
    meme_20 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][19][:min_idx]
    meme_21 = academia_to_company_top_20_memes_for_company_level['top_20_meme_names'][20][:min_idx]

    perc_1 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][0][:min_idx]
    perc_2 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][1][:min_idx]
    perc_3 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][2][:min_idx]
    perc_4 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][3][:min_idx]
    perc_5 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][4][:min_idx]
    perc_6 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][5][:min_idx]
    perc_7 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][6][:min_idx]
    perc_8 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][7][:min_idx]
    perc_9 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][8][:min_idx]
    perc_10 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][9][:min_idx]
    perc_11 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][10][:min_idx]
    perc_12 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][11][:min_idx]
    perc_13 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][12][:min_idx]
    perc_14 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][13][:min_idx]
    perc_15 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][14][:min_idx]
    perc_16 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][15][:min_idx]
    perc_17 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][16][:min_idx]
    perc_18 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][17][:min_idx]
    perc_19 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][18][:min_idx]
    perc_20 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][19][:min_idx]
    perc_21 = academia_to_company_top_20_memes_for_company_level['top_20_meme_perc'][20][:min_idx]

    academia_to_company_meme_perc_table = pd.DataFrame({ 
    'meme_1': meme_1, 'perc_1': perc_1, 
    'meme_2': meme_2, 'perc_2': perc_2, 
    'meme_3': meme_3, 'perc_3': perc_3, 
    'meme_4': meme_4, 'perc_4': perc_4, 
    'meme_5': meme_5, 'perc_5': perc_5, 
    'meme_6': meme_6, 'perc_6': perc_6, 
    'meme_7': meme_7, 'perc_7': perc_7, 
    'meme_8': meme_8, 'perc_8': perc_8, 
    'meme_9': meme_9, 'perc_9': perc_9, 
    'meme_10': meme_10, 'perc_10': perc_10, 
    'meme_11': meme_11, 'perc_11': perc_11, 
    'meme_12': meme_12, 'perc_12': perc_12, 
    'meme_13': meme_13, 'perc_13': perc_13, 
    'meme_14': meme_14, 'perc_14': perc_14, 
    'meme_15': meme_15, 'perc_15': perc_15, 
    'meme_16': meme_16, 'perc_16': perc_16, 
    'meme_17': meme_17, 'perc_17': perc_17, 
    'meme_18': meme_18, 'perc_18': perc_18, 
    'meme_19': meme_19, 'perc_19': perc_19, 
    'meme_20': meme_20, 'perc_20': perc_20,
    'meme_21': meme_21, 'perc_21': perc_21})

    print('academia_to_company_meme_perc_table', flush = True)
    print(academia_to_company_meme_perc_table, flush = True)

    print('Saving academia_to_company_meme_perc_table..', flush = True)
    academia_to_company_meme_perc_table.to_parquet('academia_to_company_meme_perc_table.parquet')
    academia_to_company_meme_perc_table.to_csv('academia_to_company_meme_perc_table.csv')

    min_idx = 30
    for i in range(len(top_20_memes_for_company_level)):
        if (len(top_20_memes_for_company_level['top_20_meme_names'][i]) < min_idx):
            min_idx = len(top_20_memes_for_company_level['top_20_meme_names'][i])


    meme_1 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][0][:min_idx]
    meme_2 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][1][:min_idx]
    meme_3 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][2][:min_idx]
    meme_4 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][3][:min_idx]
    meme_5 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][4][:min_idx]
    meme_6 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][5][:min_idx]
    meme_7 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][6][:min_idx]
    meme_8 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][7][:min_idx]
    meme_9 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][8][:min_idx]
    meme_10 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][9][:min_idx]
    meme_11 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][10][:min_idx]
    meme_12 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][11][:min_idx]
    meme_13 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][12][:min_idx]
    meme_14 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][13][:min_idx]
    meme_15 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][14][:min_idx]
    meme_16 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][15][:min_idx]
    meme_17 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][16][:min_idx]
    meme_18 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][17][:min_idx]
    meme_19 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][18][:min_idx]
    meme_20 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][19][:min_idx]
    meme_21 = company_to_academia_top_20_memes_for_company_level['top_20_meme_names'][20][:min_idx]

    perc_1 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][0][:min_idx]
    perc_2 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][1][:min_idx]
    perc_3 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][2][:min_idx]
    perc_4 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][3][:min_idx]
    perc_5 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][4][:min_idx]
    perc_6 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][5][:min_idx]
    perc_7 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][6][:min_idx]
    perc_8 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][7][:min_idx]
    perc_9 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][8][:min_idx]
    perc_10 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][9][:min_idx]
    perc_11 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][10][:min_idx]
    perc_12 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][11][:min_idx]
    perc_13 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][12][:min_idx]
    perc_14 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][13][:min_idx]
    perc_15 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][14][:min_idx]
    perc_16 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][15][:min_idx]
    perc_17 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][16][:min_idx]
    perc_18 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][17][:min_idx]
    perc_19 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][18][:min_idx]
    perc_20 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][19][:min_idx]
    perc_21 = company_to_academia_top_20_memes_for_company_level['top_20_meme_perc'][20][:min_idx]

    company_to_academia_meme_perc_table = pd.DataFrame({ 
    'meme_1': meme_1, 'perc_1': perc_1, 
    'meme_2': meme_2, 'perc_2': perc_2, 
    'meme_3': meme_3, 'perc_3': perc_3, 
    'meme_4': meme_4, 'perc_4': perc_4, 
    'meme_5': meme_5, 'perc_5': perc_5, 
    'meme_6': meme_6, 'perc_6': perc_6, 
    'meme_7': meme_7, 'perc_7': perc_7, 
    'meme_8': meme_8, 'perc_8': perc_8, 
    'meme_9': meme_9, 'perc_9': perc_9, 
    'meme_10': meme_10, 'perc_10': perc_10, 
    'meme_11': meme_11, 'perc_11': perc_11, 
    'meme_12': meme_12, 'perc_12': perc_12, 
    'meme_13': meme_13, 'perc_13': perc_13, 
    'meme_14': meme_14, 'perc_14': perc_14, 
    'meme_15': meme_15, 'perc_15': perc_15, 
    'meme_16': meme_16, 'perc_16': perc_16, 
    'meme_17': meme_17, 'perc_17': perc_17, 
    'meme_18': meme_18, 'perc_18': perc_18, 
    'meme_19': meme_19, 'perc_19': perc_19, 
    'meme_20': meme_20, 'perc_20': perc_20,
    'meme_21': meme_21, 'perc_21': perc_21})

    print('company_to_academia_meme_perc_table', flush = True)
    print(company_to_academia_meme_perc_table, flush = True)

    print('Saving company_to_academia_meme_perc_table..', flush = True)
    company_to_academia_meme_perc_table.to_parquet('company_to_academia_meme_perc_table.parquet')
    company_to_academia_meme_perc_table.to_csv('company_to_academia_meme_perc_table.csv')

if __name__=="__main__":
    typer.run(main)