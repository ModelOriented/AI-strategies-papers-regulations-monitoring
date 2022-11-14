import pandas as pd

import numpy as np
import typer
from tqdm import tqdm


def affiliations(condition_list = ['Adobe Systems', 'Alibaba Group', 'Amazon', 'Facebook','Google', 'Huawei Technologies','IBM', 'Intel','Microsoft', 'Nvidia','Samsung', 'Siemens','Tencent', 'Yahoo'],category = 'institution',big_ai_input = '/data/s2orc/big_ai_dataset.parquet',
json_input = 'data/s2orc/doi_to_authorship_big.json',output_path = 'data/s2orc/big_ai_dataset_with_affiliations.parquet'):
    
    df = pd.read_parquet(output_path)
    print(df.head)
    institutions = []
    countries = []
    types = []
    for index, row in df.iterrows():
        institutions_per_paper = []
        countries_per_paper = []
        types_per_paper = []
        if row['open_alex'] != '':
            for author in row['open_alex']:
                try:
                    institution_per_author = []
                    countries_per_author = []
                    types_per_author = []
                    for institution in author['institutions']:
                        institution_per_author.append(institution['display_name'])
                        countries_per_author.append(institution['country_code'])
                        types_per_author.append(institution['type'])
                    institutions_per_paper.append(institution_per_author)
                    countries_per_paper.append(countries_per_author)
                    types_per_paper.append(types_per_author)
                except KeyError:
                    institutions_per_paper.append("no data")
                    countries_per_paper.append("no data")
                    types_per_paper.append("no data")
        institutions.append(institutions_per_paper)
        countries.append(countries_per_paper)
        types.append(types_per_paper)


    df['institutions'] = institutions
    df['countries'] = countries
    df['types'] = types


    unique_institutions_per_paper = []
    for paper in institutions:
        unique_institutions = set()
        if paper != []:
            for author in paper:
                for affiliation in author:
                    if affiliation not in unique_institutions:
                        unique_institutions.add(affiliation)
        unique_institutions_per_paper.append(list(unique_institutions))

    df['unique_institutions'] = unique_institutions_per_paper

    unique_institutions_joint = []
    for paper in unique_institutions_per_paper:
        if paper == []:
            unique_institutions_joint.append("no data")
        else:
            for affiliation in paper:
                unique_institutions_joint.append(affiliation)


    unique_types_per_paper = []
    for paper in types:
        unique_types = set()
        if paper != []:
            for author in paper:
                for type in author:
                    if type not in unique_types:
                        if type is not None:
                            unique_types.add(type)
            unique_types = list(unique_types)
            unique_types_str = ' '.join(unique_types)
            if unique_types_str == '':
                unique_types_str = 'no data'
        else:
            unique_types_str = 'no data'
        unique_types_per_paper.append(unique_types_str)


    unique_countries_per_paper = []
    for paper in countries:
        unique_countries = set()
        if paper != []:
            for author in paper:
                for country in author:
                    if country not in unique_countries:
                        unique_countries.add(country)
        unique_countries_per_paper.append(list(unique_countries))

    unique_countries_joint = []
    for paper in unique_countries_per_paper:
        if paper == []:
            unique_countries_joint.append("no data")
        else:
            for country in paper:
                unique_countries_joint.append(country)

    is_X = []
    if category =='country':
        unique = unique_countries_per_paper
    elif category =='type':
        unique = unique_types_per_paper
    elif category == 'institution':
        unique = unique_institutions_per_paper
    for paper in unique:
        is_X_already = 0
        for institution in paper:
            if institution in condition_list:
                is_X_already = 1
        is_X.append(is_X_already)

    df['condition'] = is_X
    df.drop(columns='open_alex', inplace=True)
    df.to_parquet(output_path)

if __name__ == '__main__':
    typer.run(affiliations)