import pandas as pd
import re
import collections
import json
import numpy as np



def main(condition_list,category):
    #wczytywanie danych
    all_pd = pd.read_parquet(r'C:/Users/ppaul/Documents/AI-strategies-papers-regulations-monitoring/data/s2orc/big_ai_dataset.parquet', columns=['paper_id', 'year', 'doi', 'out_citations_count', 'in_citations_count'], engine='pyarrow')
    print(f'Number of all records: {len(all_pd)}')
    print(f'Number of unique doi records: {all_pd["doi"].nunique()}')
    print(f'Number of NAN\'s: {len(all_pd) - all_pd["doi"].nunique()}')

    df = all_pd[~((all_pd['in_citations_count'] == 0) & (all_pd['out_citations_count'] == 0))]

    file = open(r'C:/Users/ppaul/Documents/AI-strategies-papers-regulations-monitoring/data/s2orc/doi_to_authorship_big.json')
    doi_to_authorship_big = json.load(file)

    print(f'Number of dois: {len(doi_to_authorship_big.keys())}')

    keys = list(doi_to_authorship_big.keys())
    for key in keys:
        new_key = key.replace('https://doi.org/', '')
        doi_to_authorship_big[new_key] = doi_to_authorship_big.pop(key)

    df['open_alex'] = df['doi'].map(doi_to_authorship_big)
    df = df.dropna()

   
    institutions = []
    countries = []
    types = []
    for index, row in df.iterrows():
        institutions_per_paper = []
        countries_per_paper = []
        types_per_paper = []
        if row['open_alex'] != '':
            print(row['open_alex'])
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

    company = []
    education = []
    government = []
    facility = []
    healthcare = []
    nonprofit = []
    other = []
    for paper_institution, paper_type in zip(institutions, types):
        if paper_institution != [] and paper_type != []:
            institutions_so_far = set()
            for author_institution, author_type in zip(paper_institution, paper_type):
                for institution_institution, institution_type in zip(author_institution, author_type):
                    if institution_institution not in institutions_so_far:
                        institutions_so_far.add(institution_institution)
                        if institution_type == 'company':
                            company.append(institution_institution)
                        elif institution_type == 'education':
                            education.append(institution_institution)
                        elif institution_type == 'government':
                            government.append(institution_institution)
                        elif institution_type == 'healthcare':
                            healthcare.append(institution_institution)
                        elif institution_type == 'nonprofit':
                            nonprofit.append(institution_institution)
                        elif institution_type == 'facility':
                            facility.append(institution_institution)
                        elif institution_type == 'other':
                            other.append(institution_institution)

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
    df.to_parquet('data/s2orc/big_ai_dataset_with_affiliations.parquet')

#if __name__ == '__main__':
#    typer.run(main)
main(['Poland'], 'country')
