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

# # Create a text element and let the reader know the data is loading.
# data_load_state = st.text("Loading data...")
# # Load 10,000 rows of data into the dataframe.
# data = load_data(10000)
# # Notify the reader that the data was successfully loaded.
# data_load_state.text("Loading data...done!")
# st.subheader("Number of pickups by hour")
# hist_values = np.histogram(data[DATE_COLUMN].dt.hour, bins=24, range=(0, 24))[0]
# st.bar_chart(hist_values)
# if st.checkbox("Show raw data"):
#     st.subheader("Raw data")
#     st.write(data)
