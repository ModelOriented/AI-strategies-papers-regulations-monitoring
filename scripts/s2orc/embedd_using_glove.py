from typing import List
import numpy as np
import pandas as pd
import typer


def chunk_to_embedding(noun_chunks: List[str], word_embeddings: dict) -> List[np.ndarray]:
    """
    Function to embedd noun chunks. If element of the list consists of more than one word, the final embedding is the
    average of the embeddings of the words.
    """
    chunk_to_embedding_mapping = {}
    for chunk in noun_chunks:
        if ' ' in chunk:
            words = chunk.split(' ')
            chunk_to_embedding_mapping['chunk'] = (np.mean([word_embeddings[word] for word in words], axis=0))
        else:
            chunk_to_embedding_mapping['chunk'] = word_embeddings[chunk]
    return chunk_to_embedding_mapping


def read_embeddings(filename: str) -> dict:
    """
    Function to read embeddings from file.
    """
    embeddings = {}
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            print(line)
            line = line.split()
            embeddings[line[0]] = np.array(line[1:], dtype=np.float32)
    return embeddings


def process(path_to_parquet: str, path_to_embeddings: str, path_to_output: str):
    print(f'Loading parquet with noun chunks from {path_to_parquet}')
    try:
        df = pd.read_parquet(path_to_parquet)
        print(f'Loaded parquet with noun chunks from {path_to_parquet}')
        print('Length of parquet:', len(df))
    except Exception as e:
        print(e)
        print('Could not load parquet with noun chunks from', path_to_parquet)
        return

    print(f'Loading embeddings from {path_to_embeddings}')
    embeddings = read_embeddings(path_to_embeddings)
    print(f'Number of embeddings: {len(embeddings)}')

    print(f'Getting unique noun chunks from parquet')
    # Parquet column noun_chunks_cleaned contains lists of noun chunks.
    # We want to get unique noun chunks from the whole column
    all_noun_chunks = df['noun_chunks_cleaned'].tolist()
    unique_noun_chunks = set()
    all_noun_chunks_num = 0
    for noun_chunks in all_noun_chunks:
        for chunk in noun_chunks:
            all_noun_chunks_num += 1
            unique_noun_chunks.add(chunk)
    print(f'Number of unique noun chunks: {len(unique_noun_chunks)} from {all_noun_chunks_num}')

    print(f'Embedding noun chunks')
    chunk_to_embedding_mapping_dict = chunk_to_embedding(list(unique_noun_chunks), embeddings)
    print(f'Embedding noun chunks done')

    print(f'Saving parquet with embeddings to {path_to_output}')
    # Making dataframe with two columns: chunk and embedding
    df_embeddings = pd.DataFrame.from_dict(chunk_to_embedding_mapping_dict, orient='index')
    df_embeddings.to_parquet(path_to_output)
    print(f'Saving parquet with embeddings to {path_to_output} done')


if __name__ == '__main__':
    typer.run(process)
