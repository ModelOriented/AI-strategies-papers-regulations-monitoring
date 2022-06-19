import pandas as pd
import re
import typer
import networkx as nx
from pyvis.network import Network

def main(input_path:str, names_path:str, output_path:str):
    df = pd.read_csv(input_path)
    df['antecedents'] = df['antecedents'].apply(lambda x: re.findall(r'\'(.+?)\'', x))
    df['consequents'] = df['consequents'].apply(lambda x: re.findall(r'\'(.+?)\'', x))
    names = pd.read_parquet(names_path)
    antecedents_names_most_common = []
    consequents_names_most_common = []
    antecedents_names_besttf_idf = []
    consequents_names_besttf_idf = []
    for index, row in df.iterrows():
        most_common = []
        best_tfidf = []
        for i in row['antecedents']:
            temp_word_most_common = ''
            temp_word_besttf_idf = ''
            if i.startswith('i_'):
                temp_word_most_common += 'i_'
                temp_word_besttf_idf += 'i_'
                i = i.strip('i_')
                temp_word_most_common += names.iloc[int(i)]['most_common']
                temp_word_besttf_idf += names.iloc[int(i)]['best_tfidf']
            else:
                if i.isnumeric():
                    temp_word_most_common += names.iloc[int(i)]['most_common']
                    temp_word_besttf_idf += names.iloc[int(i)]['best_tfidf']
                else:
                    temp_word_most_common += i
                    temp_word_besttf_idf += i
            most_common.append(temp_word_most_common)
            best_tfidf.append(temp_word_besttf_idf)
        antecedents_names_most_common.append(most_common)
        antecedents_names_besttf_idf.append(best_tfidf)
        most_common = []
        best_tfidf = []
        for i in row['consequents']:
            temp_word_most_common = ''
            temp_word_besttf_idf = ''
            if i.startswith('i_'):
                temp_word_most_common += 'i_'
                temp_word_besttf_idf += 'i_'
                i = i.strip('i_')
                temp_word_most_common += names.iloc[int(i)]['most_common']
                temp_word_besttf_idf += names.iloc[int(i)]['best_tfidf']
            else:
                if i.isnumeric():
                    temp_word_most_common += names.iloc[int(i)]['most_common']
                    temp_word_besttf_idf += names.iloc[int(i)]['best_tfidf']
                else:
                    temp_word_most_common += i
                    temp_word_besttf_idf += i
            most_common.append(temp_word_most_common)
            best_tfidf.append(temp_word_besttf_idf)
        consequents_names_most_common.append(most_common)
        consequents_names_besttf_idf.append(best_tfidf)

    df['antecedents_names_besttf_idf'] = antecedents_names_besttf_idf
    df['antecedents_names_most_common'] = antecedents_names_most_common
    df['consequents_names_besttf_idf'] = consequents_names_besttf_idf
    df['consequents_names_most_common'] = consequents_names_most_common

    df['antecedents_names_besttf_idf'] = [','.join(map(str, l)) for l in df['antecedents_names_besttf_idf']]
    df['consequents_names_besttf_idf'] = [','.join(map(str, l)) for l in df['consequents_names_besttf_idf']]

    G = nx.from_pandas_edgelist(df, source='antecedents_names_besttf_idf', target='consequents_names_besttf_idf',
                                edge_attr='confidence')
    net = Network(notebook=True, directed=True)
    net.from_nx(G)
    net.toggle_physics(False)
    net.width = '1500px'
    net.height = '1000px'
    net.show_buttons(filter_=['nodes', 'edges', 'physics'])
    net.save_graph(output_path)

if __name__ == "__main__":
    typer.run(main)