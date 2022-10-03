import pandas as pd
import spacy
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer
from sentence_transformers import SentenceTransformer
import typer
import os
#FINAL_TABLE_PATH = 'C:/Users/Hubert/Documents/DarlingProject/Overton/EDA/final_table.parquet'
#FOLDER_PATH = 'C:/Users/Hubert/Documents/DarlingProject/Overton/BERTopic'

def topic_modelling(data, sentence_transformer, en, folder_path, nr_topics = 'auto', min_ngram = 1, max_ngram = 3,  top_n_words = 5, top_n_topics = 32, model_name = 'BERTopic_default'):

    print('Creating the model...')
    topic_model = BERTopic(embedding_model = sentence_transformer, nr_topics = nr_topics, top_n_words = top_n_words)
    topics, probs = topic_model.fit_transform(data)

    print('Modifying the model with ngrams and stop words...')
    vectorizer_model = CountVectorizer(ngram_range = (min_ngram, max_ngram), stop_words = en.Defaults.stop_words)
    topic_model.update_topics(data, topics, vectorizer_model = vectorizer_model)

    print('Saving the model...')
    model_path = os.path.join(folder_path, 'models', model_name)
    topic_model.save(model_path)

    print('Getting model info...')
    info = topic_model.get_topic_info()
    info_path = os.path.join(folder_path, 'excel', str(model_name +'_info.xlsx'))
    info.to_excel(info_path)

    print('Visualizing topics...')
    topics = topic_model.visualize_topics()
    topics_path = os.path.join(folder_path, 'plots', str(model_name + '_topics.html'))
    topics.write_html(topics_path)

    print('Creating topics barcharts...')
    bars = topic_model.visualize_barchart(top_n_topics = top_n_topics, n_words = top_n_words)
    bars_path = os.path.join(folder_path, 'plots', str(model_name + '_bars.html'))
    bars.write_html(bars_path)

    print('Creating the heatmap...')
    heatmap = topic_model.visualize_heatmap(top_n_topics = top_n_topics)
    heatmap_path = os.path.join(folder_path, 'plots', str(model_name +  '_heatmap.html'))
    heatmap.write_html(heatmap_path)

    print('Visualizing the hierarchy...')
    hierarchy = topic_model.visualize_hierarchy(top_n_topics = top_n_topics)
    hierarchy_path = os.path.join(folder_path, 'plots', str(model_name +  '_hierarchy.html'))
    hierarchy.write_html(hierarchy_path)

    print('Extracting the keywords...')
    topic_nr = []
    key_words = []
    for i in range(len(topic_model.topics) - 1) :
        topic_nr.append(i)
        words = []
        for j in range(5) :
            words.append(topic_model.topics[i][j][0])
        key_words.append(words)

    keywords = pd.DataFrame({'topic_nr' : topic_nr,
                            'key_words' : key_words})
    keywords_path = os.path.join(folder_path, 'excel',str(model_name + '_keywords.xlsx'))
    keywords.to_excel(keywords_path)

def prepare_data(final_table_path : str):
    
    print('Preparing the dataset')
    df = pd.read_parquet(final_table_path)
    df = df[['Title','Text','n_paragraphs','n_words']]

    text_paragraph = []
    for i in range(len(df)):
        if df['Text'][i] is not None:
            for j in range(len(df['Text'][i])):
                text_paragraph.append(df['Text'][i][j])

    return text_paragraph

def main(final_table_path : str,
        folder_path : str,
        sentence_transformer : str = 'sentence-transformers/all-MiniLM-L6-v2',
        nr_topics : int = -1,
        min_ngram : int = 1,
        max_ngram : int = 3,
        top_n_words : int = 5, 
        top_n_topics : int = 32, 
        model_name : str = 'BERTopic_default') :

    if nr_topics == -1:
       nr_topics = 'auto' 
    
    print('Preparing file structre...')
    try:
        os.mkdir(os.path.join(folder_path, 'excel'))
        os.mkdir(os.path.join(folder_path, 'models'))
        os.mkdir(os.path.join(folder_path, 'plots'))
    except:
        print('Structure already exists!')

    print('Loading sentence transformer...')

    df = prepare_data(final_table_path)
    
    transformer = SentenceTransformer(sentence_transformer)

    en = spacy.load("en_core_web_md")
    #en.add_pipe('spacytextblob')

    topic_modelling(df, transformer, en, folder_path, nr_topics = nr_topics, min_ngram = min_ngram, max_ngram = max_ngram,  top_n_words = top_n_words, top_n_topics = top_n_topics, model_name = model_name)

if __name__=="__main__":
    typer.run(main)


