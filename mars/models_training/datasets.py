from enum import Enum

import pandas as pd


# TODO: @Wisnia tutaj dodaj ten nowy dataset
class DocumenLevelDataset(Enum):
    ethics_ai_ethics = "hagendorffEthicsAIEthics2020"


labels_paths = {
    DocumenLevelDataset.ethics_ai_ethics: "data/labels_hagendorffEthicsAIEthics2020.csv"
}

labels = {
    DocumenLevelDataset.ethics_ai_ethics: pd.read_csv(
        labels_paths[DocumenLevelDataset.ethics_ai_ethics], index_col=0
    )
}

targets = {
    DocumenLevelDataset.ethics_ai_ethics: list(
        labels[DocumenLevelDataset.ethics_ai_ethics].index
    )
}
