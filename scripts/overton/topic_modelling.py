### Inspired by https://www.machinelearningplus.com/nlp/topic-modeling-gensim-python/
from tqdm import tqdm
import nltk
from spacy import Language

nltk.download('stopwords')
import re
import itertools
import numpy as np
import time
from typing import List
import pandas as pd
from pprint import pprint

# Gensim
import gensim
import gensim.corpora as corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel

# spacy for lemmatization
import spacy
import textacy.extract
from spacy_langdetect import LanguageDetector


@Language.component("language_detector")
def component_func(doc):
    detector = LanguageDetector()
    return detector(doc)


nlp = spacy.load("en_core_web_sm")
nlp.add_pipe("language_detector", last=True)

# Plotting tools
import pyLDAvis
import pyLDAvis.gensim_models
import matplotlib.pyplot as plt

from nltk.corpus import stopwords

stop_words = stopwords.words('english')


def preprocess_texts(texts: List[str], allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV']) -> List[List[str]]:
    texts = [re.sub('\s+', ' ', paragraph) for paragraph in tqdm(texts, desc="Remove white spaces")]
    texts_out = []
    for doc in tqdm(nlp.pipe(texts), desc='Lemmatize', total=len(texts)):
        ngrams = [x.text for x in textacy.extract.basics.ngrams(doc, 2, min_freq=2)]
        result = [token.lemma_.lower() for sent in doc.sents
                  for token in sent
                  if sent._.language['language'] == 'en' and token.pos_ in allowed_postags
                  if not token.is_stop] + ngrams
        texts_out.append(result)
    return texts_out


def main(per_paragraph=True, first=10, num_topics=6, lda_chunksize=6, save_model=False):
    tc = pd.read_parquet("../../data/overton/AI_subsample/text_col.parquet")  # All the filenames finish with ".pdf"
    proc = pd.read_parquet("../../data/overton/AI_subsample/processed.parquet")[
        ["policy_document_id", "overton_policy_document_series"]]
    # We need to drop duplicates because 191 out of 2258 values are duplicated
    types = proc[["policy_document_id", "overton_policy_document_series"]].drop_duplicates(subset="policy_document_id")
    tqdm.pandas(desc="Get document ids from filenames")
    tc["policy_document_id"] = tc.pdf_filename.progress_apply(lambda x: x.rsplit("-", 1)[0])
    tc = pd.merge(tc, types, on='policy_document_id')

    # This leaves only 2086 out of 2258 rows
    tc = tc.loc[tc["overton_policy_document_series"] == "Publication"]

    docs_split_in_paragraphs = [p.tolist() for p in tc.text.values.tolist()]
    doc_ids = tc.policy_document_id.values.tolist()
    data_with_ids = zip(docs_split_in_paragraphs[:first], doc_ids[:first])
    data_with_ids = [(paragraph, doc_id) for doc, doc_id in data_with_ids for paragraph in doc] if per_paragraph \
        else [(' '.join(doc), doc_id) for doc, doc_id in data_with_ids]
    data, doc_ids = list(zip(*data_with_ids))

    data_lemmatized = preprocess_texts(data, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV'])

    id2word = corpora.Dictionary([x for x in tqdm(data_lemmatized, desc='Create Dictionary')])

    # Create Corpus
    texts = data_lemmatized

    # Term Document Frequency
    corpus = [id2word.doc2bow(text) for text in tqdm(texts, desc='Create Bag of Words')]
    print(sorted([(freq, id2word[id]) for id, freq in corpus[0]], reverse=True)[:20])

    start_time = time.perf_counter()
    lda_model = gensim.models.LdaMulticore(corpus=corpus,
                                           id2word=id2word,
                                           num_topics=num_topics,
                                           random_state=100,
                                           alpha='auto',
                                           # update_every=1,  # Not present in multicore
                                           # chunksize=2000,
                                           # passes=1,  # By default, passes=1 is used, more was on the website
                                           # per_word_topics=False
                                           )
    print(f"LdaMulticore for {first} docs: {time.perf_counter() - start_time} seconds")
    if save_model:
        lda_model.save(f"../../models/lda_model-{first}_num-topics-{num_topics}.pkl")
    pprint(lda_model.print_topics())
    print('\nPerplexity: ', lda_model.log_perplexity(
        corpus))  # a measure of how good the model is. lower the better. SF: Really? Not higher the better?

    # Compute Coherence Score
    coherence_model_lda = CoherenceModel(model=lda_model, texts=data_lemmatized, dictionary=id2word, coherence='c_v')
    coherence_lda = coherence_model_lda.get_coherence()
    print('\nCoherence Score: ', coherence_lda)

    vis = pyLDAvis.gensim_models.prepare(lda_model, corpus, id2word)
    pyLDAvis.save_html(data=vis, fileobj=f"../../results/vis-{first}_num-topics-{num_topics}.html")

    def format_topics_sentences(ldamodel, corpus, doc_ids):
        # Init output
        sent_topics_df = pd.DataFrame()

        # Get main topic in each document
        for i, row in enumerate(ldamodel[corpus]):
            # try:
            row = row[0]
            row = sorted(row, key=lambda x: (x[1]), reverse=True)
            # except (TypeError, IndexError) as e:
            #     print(e, row)
            # Get the Dominant topic, Perc Contribution and Keywords for each document
            for j, (topic_num, prop_topic) in enumerate(row):
                if j == 0:  # => dominant topic
                    wp = ldamodel.show_topic(topic_num)
                    topic_keywords = ", ".join([word for word, prop in wp])
                    sent_topics_df = sent_topics_df.append(
                        pd.Series([int(topic_num), round(prop_topic, 4), topic_keywords]), ignore_index=True)
                else:
                    break
        sent_topics_df.columns = ['Dominant_Topic', 'Perc_Contribution', 'Topic_Keywords']

        # Add original text to the end of the output
        ids_series = pd.Series(doc_ids)
        sent_topics_df = pd.concat([sent_topics_df, ids_series], axis=1)
        return sent_topics_df

    df_topic_sents_keywords = format_topics_sentences(ldamodel=lda_model, corpus=corpus, doc_ids=doc_ids)

    # Format
    # df_dominant_topic = df_topic_sents_keywords.reset_index()
    df_topic_sents_keywords.columns = ['dominant_topic', 'topic_perc_contrib', 'keywords', 'policy_document_id']

    # Show
    print(df_topic_sents_keywords)
    df_topic_sents_keywords.to_parquet(
        f"../../data/overton/AI_subsample/topics_first-{first}_num-topics-{num_topics}.parquet")


if __name__ == '__main__':
    main()
