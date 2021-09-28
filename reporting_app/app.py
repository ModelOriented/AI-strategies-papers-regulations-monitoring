import numpy as np
import pandas as pd
import streamlit as st

from mars.db import collections, db_fields
from mars.models_training import datasets


@st.cache
def load_data():
    d = collections.analytics.exportDocs()
    return pd.DataFrame(d)


issue = st.sidebar.radio(
    "Choose the issue to explore",
    datasets.targets[datasets.DocumentLevelDataset.ethics_ai_ethics],
)


data = load_data()
st.write("## Distribution of issue in documents")
data = data[data[db_fields.QUERY_TARGET] == issue]
similarities = data[db_fields.SIMILARITY_SCORE]
hist_values = np.histogram(similarities, bins=20, range=(0, 1))
st.bar_chart(hist_values[0])
st.write(data)
print(hist_values)
st.info("This is a purely informational message")
