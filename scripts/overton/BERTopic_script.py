import pandas as pd
import spacy
from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer
from sentence_transformers import SentenceTransformer
import typer
#FINAL_TABLE_PATH = 'C:/Users/Hubert/Documents/DarlingProject/Overton/EDA/final_table.parquet'
#FOLDER_PATH = 'C:/Users/Hubert/Documents/DarlingProject/Overton/BERTopic'

def topic_modelling(data, sentence_transformer, en, folder_path, nr_topics = 'auto', min_ngram = 1, max_ngram = 3,  top_n_words = 5, top_n_topics = 32, model_name = 'BERTopic_default'):
    topic_model = BERTopic(embedding_model = sentence_transformer, nr_topics = nr_topics, top_n_words = top_n_words)
    topics, probs = topic_model.fit_transform(data)

    vectorizer_model = CountVectorizer(ngram_range = (min_ngram, max_ngram), stop_words = en.Defaults.stop_words)
    topic_model.update_topics(data, topics, vectorizer_model = vectorizer_model)

    model_path = folder_path + str('/models/') + model_name
    topic_model.save(model_path)

    info = topic_model.get_topic_info()
    info_path = folder_path + str('/excel/') + model_name + str('_info.xlsx')
    info.to_excel(info_path)

    topics = topic_model.visualize_topics()
    topics_path = folder_path + str('/plots/') + model_name + str('_topics.html')
    topics.write_html(topics_path)

    bars = topic_model.visualize_barchart(top_n_topics = top_n_topics, n_words = top_n_words)
    bars_path = folder_path + str('/plots/') + model_name + str('_bars.html')
    bars.write_html(bars_path)

    heatmap = topic_model.visualize_heatmap(top_n_topics = top_n_topics)
    heatmap_path = folder_path + str('/plots/') + model_name + str('_heatmap.html')
    heatmap.write_html(heatmap_path)

    hierarchy = topic_model.visualize_hierarchy(top_n_topics = top_n_topics)
    hierarchy_path = folder_path + str('/plots/') + model_name + str('_hierarchy.html')
    hierarchy.write_html(hierarchy_path)

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
    keywords_path = folder_path + str('/excel/') + model_name + str('_keywords.xlsx')
    keywords.to_excel(keywords_path)

def prepare_data(final_table_path : str):
    df = pd.read_parquet(final_table_path)
    df = df[['Title','Text','n_paragraphs','n_words']]

    Text_paragraph = []
    for i in range(len(df)):
        if df['Text'][i] is not None:
            for j in range(len(df['Text'][i])):
                Text_paragraph.append(df['Text'][i][j])

    return Text_paragraph

def main(final_table_path : str,
        folder_path : str,
        nr_topics : int = -1,
        min_ngram : int = 1,
        max_ngram : int = 3,
        top_n_words : int = 5, 
        top_n_topics : int = 32, 
        model_name : str = 'BERTopic_default') :

    if nr_topics == -1:
       nr_topics = 'auto' 

    df = prepare_data(final_table_path)

    MiniLM = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

    en = spacy.load("en_core_web_md")
    #en.add_pipe('spacytextblob')

    topic_modelling(df, MiniLM, en, folder_path, nr_topics = nr_topics, min_ngram = min_ngram, max_ngram = max_ngram,  top_n_words = top_n_words, top_n_topics = top_n_topics, model_name = model_name)

if __name__=="__main__":
    typer.run(main)


