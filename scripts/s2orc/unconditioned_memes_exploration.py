import pandas as pd
import numpy as np
import memes_pipeline

if __name__ == '__main__':
    
    clustering_path = ['/reduced_300_big_cleaned_phrase-bert_eps_0.2_min_clust_size_3.parquet',
                        '/reduced_300_big_cleaned_phrase-bert_eps_0.3_min_clust_size_3.parquet',
                        '/reduced_300_big_cleaned_phrase-bert_eps_0.4_min_clust_size_3.parquet']
    for path in clustering_path:

        memes_pipeline.pipeline(None,
                'data/s2orc/chunk_meme_mappings'+path, 
                'data/s2orc/meme_score'+path, 
                aff_output_path = 'data/s2orc/big_ai_dataset_with_affiliations_extended_oa.parquet',
                condition_list = ['company'],
                category = 'types',
                do_cluster = False)