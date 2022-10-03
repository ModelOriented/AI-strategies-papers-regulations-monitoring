import pandas as pd
import typer

def main(overton_excel_path : str, duplicate_only_path : str, no_duplicates_path : str, duplicate_report_path : str, nr_of_examples : int):
    '''
    Conducts a Duplicate filtering based on Overton xlsx table

    The filtering is done by 4 rules:
    Rule 1: Two observations have the same PDF URL or URL.
    Rule 2: Two observations have the same Published date and the last URL chunk (/xyz.pdf).
    Rule 3: Two observations have the same Published date and the last PDF URL chunk (/xyz.pdf).
    Rule 4: Two observations have the same Published date and the same Title.

    Parameters:
    overton_excel_path    : str - path to the Overton xlsx file
    duplicate_only_path   : str - path where the Data Frame with duplicate rows will be saved (.parquet required)
    no_duplicates_path    : str - path where the Data Frame without duplicate rows will be saved (.parquet required)
    duplicate_report_path : str - path where the filtering report will be saved (.txt required)
    nr_of_examples        : int - number of examples inside the report
    '''

    df = pd.read_excel(overton_excel_path)
    df = df[['Title', 'Published', 'URL', 'PDF URL']]

    df['Duplicate'] = [False] * len(df)

    nr_rule_1 = 0 # Rule 1: same PDF URL or URL.
    nr_rule_2 = 0 # Rule 2: same Published date and the last URL chunk (/xyz.pdf).
    nr_rule_3 = 0 # Rule 3: same Published date and the last PDF URL chunk (/xyz.pdf).
    nr_rule_4 = 0 # Rule 4: same Published date and the same Title.
    pairs     = [] 

    for idx, row in df.iterrows() :
        if idx % 100 == 0 :
            print(idx)
        if row['Duplicate'] == False :

            title        = row[0]
            published    = row[1]
            url          = row[2]
            pdf_url      = row[3]

            for j in range(idx + 1, len(df)) :
                if url == df.iloc[j, 2] or pdf_url == df.iloc[j, 3] : # Rule 1
                    nr_rule_1 += 1
                    pairs.append((idx, j, 1))
                    df['Duplicate'][j] = True

                if not pd.isna(url) and not pd.isna(df.iloc[j, 2]) and published == df.iloc[j, 1] and url.rsplit('/', 1)[-1] == df.iloc[j, 2].rsplit('/', 1)[-1] and url.rsplit('/', 1)[-1][-4:] == '.pdf': # Rule 2
                    nr_rule_2 += 1
                    pairs.append((idx, j, 2))
                    df['Duplicate'][j] = True
                
                if not pd.isna(pdf_url) and not pd.isna(df.iloc[j, 3]) and published == df.iloc[j, 1] and pdf_url.rsplit('/', 1)[-1] == df.iloc[j, 3].rsplit('/', 1)[-1] and pdf_url.rsplit('/', 1)[-1][-4:] == '.pdf': # Rule 3
                    nr_rule_3 += 1
                    pairs.append((idx, j, 3))
                    df['Duplicate'][j] = True

                if published == df.iloc[j, 1] and title == df.iloc[j, 0] : # Rule 4
                    nr_rule_4 += 1
                    pairs.append((idx, j, 4))
                    df['Duplicate'][j] = True

    with open(duplicate_report_path, 'a') as file_object:
        file_object.write('Nr of rule 1 cases ' + str(nr_rule_1) + '\n')
        file_object.write('Nr of rule 2 cases ' + str(nr_rule_2) + '\n')
        file_object.write('Nr of rule 3 cases ' + str(nr_rule_3) + '\n')
        file_object.write('Nr of rule 4 cases ' + str(nr_rule_4) + '\n')
        file_object.write('Number of duplicates ' + str(len(df[df['Duplicate'] == True])) + '\n')
        file_object.write('Examples:' + '\n')
        file_object.write('\n')
        for i in range(min(len(pairs), nr_of_examples)) :
            file_object.write(df.iloc[pairs[i][0]]['Title'] + '\n')
            file_object.write(df.iloc[pairs[i][1]]['Title'] + '\n')
            file_object.write(df.iloc[pairs[i][0]]['URL'] + '\n')
            file_object.write(df.iloc[pairs[i][1]]['URL'] + '\n')
            file_object.write(df.iloc[pairs[i][0]]['PDF URL'] + '\n')
            file_object.write(df.iloc[pairs[i][1]]['PDF URL'] + '\n')
            file_object.write('\n')

    dupl_only = df[df['Duplicate'] == True]
    dupl_only.to_parquet(duplicate_only_path)

    no_dupl = df[df['Duplicate'] == False]
    no_dupl.to_parquet(no_duplicates_path)

if __name__=="__main__":
    typer.run(main)