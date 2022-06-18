import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
import typer

def main(input_path:str, output_path:str):
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
    df = df[df['institutions'].map(lambda d: len(d)) > 0]
    print("Droping columns ...")
    counts = df.sum(axis=0)
    columns_to_drop = counts[counts == 1].index
    df_small = df.drop(list(columns_to_drop), axis=1)
    print('Transaction encoding ...')
    te = TransactionEncoder()
    te_ary = te.fit(df_small['baskets_inbound']).transform(df_small['baskets_inbound'])
    transactions_df = pd.DataFrame(te_ary, columns=te.columns_)
    print('Saving ...')
    transactions_df.to_parquet(output_path)

if __name__ == "__main__":
    typer.run(main)
