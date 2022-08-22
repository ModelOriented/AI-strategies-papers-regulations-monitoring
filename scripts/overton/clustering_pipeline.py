from scripts.overton import process_spacy
from scripts.s2orc import memes_pipeline

import typer

def main():
    process_spacy.main('data/overton/AI_subsample/processed.parquet', 
                        'data/overton/AI_subsample/processed_noun_chunks.parquet',
                        spacy_model_name = 'en_core_web_sm')
    memes_pipeline.main(   out_path_embedding = 'data/overton/embedding/all-MiniLM-L6-v2.parquet',
                out_path_cluster = 'data/overton/clustering/clustering1.parquet', 
                out_path_meme_score=None, 
                in_path = 'data/overton/AI_subsample/processed_noun_chunks.parquet',
                do_memes = False
                )

    
if __name__ == '__main__':
    typer.run(main)