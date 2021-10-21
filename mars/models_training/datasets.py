from enum import Enum

import pandas as pd



class DocumentLevelDataset(Enum):
    ethics_ai_ethics = "hagendorffEthicsAIEthics2020"
    jobin2019 = "jobin2019"

labels_paths = {
    DocumentLevelDataset.ethics_ai_ethics: "data/labels_hagendorffEthicsAIEthics2020.csv",
    DocumentLevelDataset.jobin2019: "data/jobin2019.csv"
}

labels = {
    DocumentLevelDataset.ethics_ai_ethics: pd.read_csv(
        labels_paths[DocumentLevelDataset.ethics_ai_ethics], index_col=0
    ),
    DocumentLevelDataset.jobin2019: pd.read_csv(
            labels_paths[DocumentLevelDataset.jobin2019], index_col = 0
        )
}

targets = {
    DocumentLevelDataset.ethics_ai_ethics: list(
        labels[DocumentLevelDataset.ethics_ai_ethics].index
    ),
    DocumentLevelDataset.jobin2019: list(
        labels[DocumentLevelDataset.jobin2019].columns
    )
}
