import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer


def main(df: pd.DataFrame, delta=0.0001):
    #the columns of citations inside graph
    inc=[]
    out=[]
    for _,row in df.iterrows():
        cit =[]
        for cited in row['outbound_citations']:
            cited = int(cited)
            if cited in df['paper_id']:
                cit.append(cited) 
        out.append(cit)
        cit=[]
        for citing in row['inbound_citations']:
            citing = int(citing)
            if citing in df['paper_id']:
                cit.append(citing)
        inc.append(cit)
    df['inbound_citations_clear']=inc
    df['outbound_citations_clear']=out

    #OneHotEncoding of memes
    enc = MultiLabelBinarizer(sparse_output=True)
    memes_enc = enc.fit_transform(df['noun_chunks_cleaned'])


    #new column of unique noun chunks in all cited papers
    memes_in_cited = []
    for i,paper in df.iterrows():
        if paper['outbound_citations_clear'].size>0:
            #only memes in cited papers:
            c = df.iloc[paper['outbound_citations_clear']]['noun_chunks_cleaned']
            
            memes_in_cited.append(list(set(c.explode())))
        else:
            memes_in_cited.append(list(set()))

    df['memes_in_cited']=memes_in_cited

    #OneHotEncoding of memes in cited papers
    c_enc = MultiLabelBinarizer(classes = enc.classes_, sparse_output=True)
    cited_memes_enc = c_enc.fit_transform(a)

    #factors for meme score
    stick2 = cited_memes_enc.sum(axis=0)
    spark2 = cited_memes_enc.shape[1] - c1#(1-cited_memes_enc).sum(axis=1)
    p = memes_enc.multiply(cited_memes_enc)
    stick1 = p.sum(axis=0)
    spark1 = memes_enc.sum(axis=0)*memes_enc.shape[1]-stick1#(1-cited_memes_enc).multiply(memes_enc).sum(axis=1) 

    frequency = memes_enc.sum(axis=0)
    propagation_factor = np.divide(np.divide(stick1,stick2+delta),np.divide(spark1+delta,spark2+delta))
    return frequency.multiply(propagation_factor)
