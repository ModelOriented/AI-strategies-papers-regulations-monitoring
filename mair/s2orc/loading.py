import os
import pandas as pd
import mair.utils
ROOT_DIR = mair.utils.ROOT_DIR

def load_big_s2orc_ai():
    return pd.read_parquet(os.path.join(ROOT_DIR, 'data/s2orc/s2orc_ai_big.parquet'))

def load_s2orc_prefiltered():
    print(os.path.join(ROOT_DIR, 'data/s2orc/s2orc_ai_prefiltered.csv'))
    return pd.read_csv(os.path.join(ROOT_DIR, 'data/s2orc/s2orc_ai_prefiltered.csv'))
