from enum import Enum

import pandas as pd


# TODO: @Wisnia tutaj dodaj ten nowy dataset
class DocumentLevelDataset(Enum):
    ethics_ai_ethics = "hagendorffEthicsAIEthics2020"


labels_paths = {
    DocumentLevelDataset.ethics_ai_ethics: "data/labels_hagendorffEthicsAIEthics2020.csv"
}

labels = {
    DocumentLevelDataset.ethics_ai_ethics: pd.read_csv(
        labels_paths[DocumentLevelDataset.ethics_ai_ethics], index_col=0
    )
}

targets = {
    DocumentLevelDataset.ethics_ai_ethics: list(
        labels[DocumentLevelDataset.ethics_ai_ethics].index
    )
}
