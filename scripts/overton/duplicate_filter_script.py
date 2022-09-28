import pandas as pd
import typer

def main(overton_excel_path : str, duplicate_only_path : str, no_duplicates_path : str, duplicate_report_path : str):

    data = pd.read_excel(overton_excel_path)
    df   = pd.DataFrame(data)
    df   = df[['Title', 'Published', 'URL', 'PDF URL']]

    df['Duplicate'] = [False] * len(df)

    nr_rule_1 = 0
    nr_rule_2 = 0
    nr_rule_3 = 0
    nr_rule_4 = 0
    pairs     = []    
    for i in range(len(df)) :
        if i % 100 == 0 :
            print(i)
        if df['Duplicate'][i] == False :

            title        = df.iloc[i, 0]
            published    = df.iloc[i, 1]
            url          = df.iloc[i, 2]
            pdf_url      = df.iloc[i, 3]

            for j in range(i + 1, len(df)) :
                if url == df.iloc[j, 2] or pdf_url == df.iloc[j, 3] :
                    nr_rule_1 += 1
                    pairs.append((i, j, 1))
                    df['Duplicate'][j] = True

                if not pd.isna(url) and not pd.isna(df.iloc[j, 2]) and published == df.iloc[j, 1] and url.rsplit('/', 1)[-1] == df.iloc[j, 2].rsplit('/', 1)[-1] and url.rsplit('/', 1)[-1][-4:] == '.pdf':
                    nr_rule_2 += 1
                    pairs.append((i, j, 2))
                    df['Duplicate'][j] = True
                
                if not pd.isna(pdf_url) and not pd.isna(df.iloc[j, 3]) and published == df.iloc[j, 1] and pdf_url.rsplit('/', 1)[-1] == df.iloc[j, 3].rsplit('/', 1)[-1] and pdf_url.rsplit('/', 1)[-1][-4:] == '.pdf':
                    nr_rule_3 += 1
                    pairs.append((i, j, 3))
                    df['Duplicate'][j] = True

                if published == df.iloc[j, 1] and title == df.iloc[j, 0] :
                    nr_rule_4 += 1
                    pairs.append((i, j, 4))
                    df['Duplicate'][j] = True

    with open(duplicate_report_path, 'a') as file_object:
        file_object.write('Nr of rule 1 cases ' + str(nr_rule_1) + '\n')
        file_object.write('Nr of rule 2 cases ' + str(nr_rule_2) + '\n')
        file_object.write('Nr of rule 3 cases ' + str(nr_rule_3) + '\n')
        file_object.write('Nr of rule 4 cases ' + str(nr_rule_4) + '\n')
        file_object.write('Number of duplicates ' + str(len(df[df['Duplicate'] == True])) + '\n')
        file_object.write('Examples:' + '\n')
        file_object.write('\n')
        file_object.write(df.iloc[15]['URL'] + '\n')
        file_object.write(df.iloc[18]['URL'] + '\n')
        file_object.write('\n')
        file_object.write(df.iloc[64]['URL'] + '\n')
        file_object.write(df.iloc[269]['URL'] + '\n')
        file_object.write('\n')
        file_object.write(df.iloc[692]['Title'] + '\n')
        file_object.write(df.iloc[842]['Title'] + '\n')
        file_object.write('\n')
        file_object.write(df.iloc[823]['PDF URL'] + '\n')
        file_object.write(df.iloc[997]['PDF URL'] + '\n')
        file_object.write('\n')
        file_object.write(df.iloc[861]['URL'] + '\n')
        file_object.write(df.iloc[915]['URL'] + '\n')

    dupl_only = df[df['Duplicate'] == True]
    dupl_only.to_parquet(duplicate_only_path)

    no_dupl = df[df['Duplicate'] == False]
    no_dupl.to_parquet(no_duplicates_path)

if __name__=="__main__":
    typer.run(main)