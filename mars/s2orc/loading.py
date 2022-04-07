import os
import pandas as pd
import mars.utils
ROOT_DIR = mars.utils.ROOT_DIR

def load_s2orc_prefiltered():
    print(os.path.join(ROOT_DIR, 'data/s2orc/s2orc_ai_prefiltered.csv'))
    return pd.read_csv(os.path.join(ROOT_DIR, 'data/s2orc/s2orc_ai_prefiltered.csv'))
