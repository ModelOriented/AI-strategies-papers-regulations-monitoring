import sys  
import pandas as pd
from collections import Counter
import numpy as np
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import spacy
import seaborn as sns
import typer
import os

#EDA_FOR_NLP_PATH = 'C:/Users/Hubert/Dropbox/DarlingProject/eda_for_nlp_package'
#FINAL_TABLE_PATH = 'C:/Users/Hubert/Documents/DarlingProject/Overton/EDA/final_table.parquet'
#SPACY_TABLE_PATH = 'C:/Users/Hubert/Documents/DarlingProject/Overton/EDA/spacy_table_new.parquet'

def main(eda_for_nlp_path : str, final_table_path : str, spacy_table_path : str):

    print('Loading the model...')
    sys.path.insert(0, eda_for_nlp_path)
    for p in sys.path:
        print( p )
    print(os.getcwd())
    import eda_for_nlp_package as eda
    en = spacy.load("en_core_web_md")

    print('Loading data...')
    df       = pd.read_parquet(final_table_path)
    print(f'df cols {df.columns}')
    df       = df[['title','text','n_paragraphs','n_words']]
    spacy_df = pd.read_parquet(spacy_table_path)
    print(f'spacy_df cols {spacy_df.columns}')


    print('Creating the text report...')
    with open("EDA_report.txt", "w") as file_object:
        file_object.write("This is report for string values. \n")

    text_all = []
    text_paragraph = []
    for i in range(len(df)):
        if df['text'][i] is not None:
            text_all.append(df['text'][i].sum())
            for j in range(len(df['text'][i])):
                text_paragraph.append(df['text'][i][j])
        else:
            text_all.append("")
    
    df['text_all'] = text_all

    with open("EDA_report.txt", "a") as file_object:
        file_object.write("Number of paragraphs:" + str(len(text_paragraph)) + "\n")
    len(text_paragraph)

    print('Preparing statistics table...')
    titles  = []
    n_ai    = []
    n_ml    = []
    n_words = []
    ai_fq   = []
    ml_fq   = []
    for i in range(len(df)):

        titles.append(df['title'][i])

        if (df['text_all'][i] != '') :
            n_ai.append(df['text_all'][i].count('artificial intelligence'))
            n_ml.append(df['text_all'][i].count('machine learning'))
            n_words.append(len(df['text_all'][i]))
        else : # for empty text files
            n_ai.append(0)
            n_ml.append(0)
            n_words.append(0)

        ai_fq.append(n_ai[i] / (n_words[i] + 1) * 100)
        ml_fq.append(n_ml[i] / (n_words[i] + 1) * 100)
    stats = pd.DataFrame({"title": titles, "n_ai": n_ai, "n_ml": n_ml, "n_words": n_words, "ai_fq(%)": ai_fq, "ml_fq(%)": ml_fq})
    stats.to_excel('excel/stats.xlsx')
    stats

    print('Calculating AI and ML frequencies...')
    ai01    = len(stats.loc[stats['ai_fq(%)'] >= 0.1])
    ai001   = len(stats.loc[stats['ai_fq(%)'] >= 0.01])
    ai0001  = len(stats.loc[stats['ai_fq(%)'] >= 0.001])
    ai00001 = len(stats.loc[stats['ai_fq(%)'] >= 0.0001])
    ml01    = len(stats.loc[stats['ml_fq(%)'] >= 0.1])
    ml001   = len(stats.loc[stats['ml_fq(%)'] >= 0.01])
    ml0001  = len(stats.loc[stats['ml_fq(%)'] >= 0.001])
    ml00001 = len(stats.loc[stats['ml_fq(%)'] >= 0.0001])

    print('AI over: ')
    print('0.1:', ai01, ', percentage: ', round(ai01 / len(df),2))
    print('0.01:', ai001, ', percentage: ', round(ai001 / len(df),2))
    print('0.001:', ai0001, ', percentage: ', round(ai0001 / len(df),2))
    print('0.0001:', ai00001, ', percentage: ', round(ai00001 / len(df),2))
    print('ML over: ')
    print('0.1:', ml01, ', percentage: ', round(ml01 / len(df),2))
    print('0.01:', ml001, ', percentage: ', round(ml001 / len(df),2))
    print('0.001:', ml0001, ', percentage: ', round(ml0001 / len(df),2))
    print('0.0001:', ml00001, ', percentage: ', round(ml00001 / len(df),2))

    with open("EDA_report.txt", "a") as file_object:
        file_object.write('AI over: \n')
        file_object.write('0.1:'+ str(ai01) + ', percentage: ' + str(round(ai01 / len(df),2)) + '\n')
        file_object.write('0.01:'+ str(ai001) + ', percentage: ' + str(round(ai001 / len(df),2)) + '\n')
        file_object.write('0.001:'+ str(ai0001) + ', percentage: ' + str(round(ai0001 / len(df),2)) + '\n')
        file_object.write('0.0001:'+ str(ai00001) + ', percentage: ' + str(round(ai00001 / len(df),2)) + '\n')
        file_object.write('ML over: \n')
        file_object.write('0.1:'+ str(ml01) + ', percentage: ' + str(round(ml01 / len(df),2)) + '\n')
        file_object.write('0.01:'+ str(ml001) + ', percentage: ' + str(round(ml001 / len(df),2)) + '\n')
        file_object.write('0.001:'+ str(ml0001) + ', percentage: ' + str(round(ml0001 / len(df),2)) + '\n')
        file_object.write('0.0001:'+ str(ml00001) + ', percentage: ' + str(round(ml00001 / len(df),2)) + '\n')
    
    print('Creating histogram...')
    df['n_paragraphs'].hist().write_image(file = 'plots/hist_n_paragraphs.png', format = 'png')
    df['n_paragraphs'].hist()

    df['n_words'].hist().write_image(file = 'plots/hist_n_words.png', format = 'png')
    df['n_words'].hist()
    
    print('Counting Languages...')
    count_language = Counter(spacy_df['paragraph_language'][0])
    for i in range(1, len(spacy_df)):
        count_language += Counter(spacy_df['paragraph_language'][i])
    count_language = eda.count_texts(count_language)
    count_language.to_excel('excel/count_language.xlsx')
    count_language

    print('Counting nouns, lemmas and noun chunks...')
    nouns = []
    for i in range(len(spacy_df['merged_nouns'])):
        nouns = np.concatenate((nouns,spacy_df['merged_nouns'][i]))
    lemmas = []
    for i in range(len(spacy_df['merged_lemmas'])):
        lemmas = np.concatenate((lemmas,spacy_df['merged_lemmas'][i]))
    noun_chunks = []
    for i in range(len(spacy_df['merged_noun_chunks'])):
        noun_chunks = np.concatenate((noun_chunks,spacy_df['merged_noun_chunks'][i]))

    with open("EDA_report.txt", "a") as file_object:
        file_object.write('Number of nouns:'+ str(len(nouns)) + '\n')
        file_object.write('Number of lemmas:'+ str(len(lemmas)) + '\n')
        file_object.write('Number of noun_chunks:'+ str(len(noun_chunks)) + '\n')

    print('Number of nouns:', len(nouns))
    print('Number of lemmas:', len(lemmas))
    print('Number of noun_chunks:', len(noun_chunks))

    print('Creating a word cloud...')
    count_nouns = eda.count_texts(nouns)
    eda.plot_counts(count_nouns, ['obj', 'count']).write_image(file = 'plots/bar_counted_nouns.png', format = 'png')
    eda.plot_counts(count_nouns, ['obj', 'count'])

    word_counts = Counter(lemmas)
    wc = WordCloud(width=1600, height=800)
    wc.generate_from_frequencies(frequencies = word_counts)
    plt.figure(figsize = (18, 14))
    plt.imshow(wc)
    plt.savefig('plots/word_cloud.png')

    print('Visualizing lemmas counts')
    count_lemmas= eda.count_texts(lemmas)
    eda.plot_counts(count_lemmas, ['obj', 'count']).write_image(file = 'plots/bar_counted_lemmas.png', format = 'png')
    eda.plot_counts(count_lemmas, ['obj', 'count'])

    print('Visualizing bigrams')
    stopwords = en.Defaults.stop_words
    top_n_bigrams=eda.get_top_ngram(df['text_all'], stopwords = stopwords, n = 2, m = 2)[:30]
    x, y=map(list,zip(*top_n_bigrams))
    fig = plt.subplots(figsize=(15, 15))
    sns.barplot(x = y, y = x)
    plt.savefig('plots/bigram.png')

    print('Visualizing trigrams')
    top_n_bigrams=eda.get_top_ngram(df['text_all'], stopwords = stopwords , n = 3, m = 3)[:30]
    x, y=map(list,zip(*top_n_bigrams))
    fig = plt.subplots(figsize=(15, 15))
    sns.barplot(x = y, y = x)
    plt.savefig('plots/trigram.png')

    print('Visualizing fourgrams')
    top_n_bigrams=eda.get_top_ngram(df['text_all'], stopwords = stopwords, n = 4, m = 4)[:30]
    x, y=map(list,zip(*top_n_bigrams))
    fig = plt.subplots(figsize=(15, 15))
    sns.barplot(x = y, y = x)
    plt.savefig('plots/fourgram.png')

    print('Visualizing most common 120 chunks...')
    noun_chunks = list(filter(lambda x: len(x.split()) > 1, noun_chunks))
    count_chunks = eda.count_texts(noun_chunks,['chunk', 'count'], 40)
    eda.plot_counts(count_chunks, ['chunk', 'count']).write_image(file = 'plots/bar_counted_chunks_from_1_to_40.png', format = 'png')
    eda.plot_counts(count_chunks,['chunk', 'count'])

    count_chunks = eda.count_texts(noun_chunks,['chunk', 'count'], 80)[40:]
    eda.plot_counts(count_chunks, ['chunk', 'count']).write_image(file = 'plots/bar_counted_chunks_from_41_to_80.png', format = 'png')
    eda.plot_counts(count_chunks,['chunk', 'count'])

    count_chunks = eda.count_texts(noun_chunks,['chunk', 'count'], 120)[80:]
    eda.plot_counts(count_chunks, ['chunk', 'count']).write_image(file = 'plots/bar_counted_chunks_from_81_to_120.png', format = 'png')
    eda.plot_counts(count_chunks,['chunk', 'count'])

    print('EDA ready')

if __name__=="__main__":
    typer.run(main)
