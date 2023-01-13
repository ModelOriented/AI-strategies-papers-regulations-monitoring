import pandas as pd
import numpy as np
import memes_pipeline

if __name__ == '__main__':
    
    clustering_path = ['/reduced_300_big_cleaned_phrase-bert_eps_0.0_min_clust_size_3.parquet',
                        '/reduced_300_big_cleaned_phrase-bert_eps_0.1_min_clust_size_3.parquet',
                        '/reduced_300_big_cleaned_phrase-bert_eps_0.0_min_clust_size_5.parquet',
                        '/reduced_300_big_cleaned_mini_all-MiniLM-L6-v2_eps_0.0_min_clust_size_3.parquet']
    for path in clustering_path:

        memes_pipeline.pipeline(None,
                'data/s2orc/clusterings'+path, 
                'data/s2orc/meme_score'+path, 
                aff_output_path = 'data/s2orc/big_ai_dataset_with_affiliations_extended_oa.parquet',
                condition_list = [],
                category = None,
                do_cluster = False)
