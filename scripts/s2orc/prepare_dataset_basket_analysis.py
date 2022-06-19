import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
import typer
from tqdm import tqdm
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

def main(input_path:str, output_path:str, min_support:float, only_aff:bool):
    print("Data loading ...")
    df = pd.read_parquet(input_path)
    print("Preparing dataset ...")
    is_academia = []
    is_company = []
    for index, paper in df.iterrows():
        if len(paper['types']) == 0:
            is_academia.append(0)
            is_company.append(0)
        else:
            paper_types = set()
            for author in paper['types']:
                for aff in author:
                    paper_types.add(aff)
            if 'company' in paper_types:
                is_company.append(1)
            else:
                is_company.append(0)
            if 'education' in paper_types or 'facility' in paper_types or 'government' in paper_types:
                is_academia.append(1)
            else:
                is_academia.append(0)

    df['is_academia'] = is_academia
    df['is_company'] = is_company
    FAANG = {'Adobe Systems': [], 'Alibaba Group': [], 'Amazon': [], 'Facebook': [], 'Google': [],
             'Huawei Technologies': [], 'IBM': [], 'Intel': [], 'Microsoft': [], 'Nvidia': [], 'Samsung': [],
             'Siemens': [], 'Tencent': [], 'Yahoo': []}

    for index, paper in df.iterrows():
        for key in FAANG.keys():
            if key in paper['unique_institutions']:
                FAANG[key].append(1)
            else:
                FAANG[key].append(0)

    for key in FAANG.keys():
        df[key] = FAANG[key]

    baskets_inbound_all = []
    for index, paper in df.iterrows():
        baskets_inbound = []
        for meme in paper['memes']:
            baskets_inbound.append(str(meme))
        for meme in paper['inbound_memes']:
            baskets_inbound.append('i_' + str(meme))
        if paper['is_big_tech'] == 1:
            baskets_inbound.append('bigtech')
            for key in FAANG.keys():
                if key in paper['unique_institutions']:
                    baskets_inbound.append(key)
        if paper['is_academia'] == 1:
            baskets_inbound.append('academia')
        if paper['is_company'] == 1:
            baskets_inbound.append('company')
        baskets_inbound_all.append(baskets_inbound)
    df['baskets_inbound'] = baskets_inbound_all
    if only_aff:
        df = df[df['institutions'].map(lambda d: len(d)) > 0]
    print('Transaction encoding ...')
    te = TransactionEncoder()
    te_ary = te.fit_transform(df['baskets_inbound'], sparse=True)
    transactions_df = pd.DataFrame.sparse.from_spmatrix(te_ary, columns=te.columns_)
    print('Aprori ...')
    itemsets = apriori(transactions_df,
                       use_colnames=True,
                       verbose=1,
                       low_memory=True,
                       min_support=min_support
                       )
    print('Association rules ...')
    rules = association_rules(itemsets, metric="lift", min_threshold=0.5)
    print('Saving ...')
    rules.to_csv(output_path)
    # print("Droping columns ...")
    # counts = transactions_df.sum(axis=0)
    # columns_to_drop = counts[counts == 1].index
    # transactions_df.drop(list(columns_to_drop), axis=1, inplace=True)
    # print('Saving ...')
    # transactions_df.to_dense().to_parquet(output_path)

if __name__ == "__main__":
    typer.run(main)
