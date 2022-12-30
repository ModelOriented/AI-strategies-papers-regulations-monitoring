import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer

def meme_score(df: pd.DataFrame, delta:float=3):
    #df['outbound_memes'] = clean_outbound_citations(df)

    print('OneHot Encoding ..')
    #OneHotEncoding of memes
    enc = MultiLabelBinarizer(sparse_output=True)
    memes_enc = enc.fit_transform(df['memes_ids'])#shape:papers x memes

    #OneHotEncoding of memes in cited papers
    c_enc = MultiLabelBinarizer(classes = enc.classes_, sparse_output=True)
    cited_memes_enc = c_enc.fit_transform(df['outbound_memes']) 
    print('Meme score ...')
    #factors for meme score

    stick2 = cited_memes_enc.sum(axis=0) #sum of papers that cite papers with this meme 
    p = memes_enc.multiply(cited_memes_enc)
    stick1 = p.sum(axis=0)#sum of papers that cite papers with this meme AND have this meme

    spark2 = cited_memes_enc.shape[0] - cited_memes_enc.sum(axis=0)#sum of papers that DO NOT cite papers with this meme
    #(1-cited_memes_enc).multiply(memes_enc).sum(axis=1) 
    spark1 = memes_enc.sum(axis=0)-memes_enc.multiply(cited_memes_enc).sum(axis=0) #sum of papers that DO NOT cite papers with this meme AND have this meme

    
    frequency = memes_enc.sum(axis=0)
    print(np.squeeze(np.array(frequency)))
    propagation_factor = np.divide(np.divide(stick1,stick2+delta),np.divide(spark1+delta,spark2+delta))

    print({'meme_id': len(enc.classes_), 'frequency': len(np.squeeze(np.array(frequency))),
                                'meme_score': len(np.squeeze(np.array(np.multiply(propagation_factor,frequency)))),
                                'sticking_factor': len(np.squeeze(np.array(np.divide(stick1,stick2+delta)))),
                                'sparking_factor': len(np.squeeze(np.array(np.divide(spark1+delta,spark2+delta)))),
                                'stick1': len(np.squeeze(np.array(stick1))),
                                'stick2': len(np.squeeze(np.array(stick2))),
                                'spark1': len(np.squeeze(np.array(spark1))),
                                'spark2': len(np.squeeze(np.array(spark2)))
                                })

    df_memes = pd.DataFrame({'meme_id': enc.classes_, 'frequency': np.squeeze(np.array(frequency)),
                                'meme_score': np.squeeze(np.array(np.multiply(propagation_factor,frequency))),
                                'sticking_factor': np.squeeze(np.array(np.divide(stick1,stick2+delta))),
                                'sparking_factor': np.squeeze(np.array(np.divide(spark1+delta,spark2+delta))),
                                'stick1': np.squeeze(np.array(stick1)),
                                'stick2': np.squeeze(np.array(stick2)),
                                'spark1': np.squeeze(np.array(spark1)),
                                'spark2': np.squeeze(np.array(spark2))
                                })
    return df_memes