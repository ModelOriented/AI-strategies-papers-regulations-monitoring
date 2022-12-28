# %%
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.colors import LogNorm, Normalize

# %%
dfff = pd.read_parquet('C:/Users/Hubert/Documents/DarlingProject/s2orc_scripts/November_paper/big_ai_dataset_with_affiliations_nb.parquet')
# %%
df = pd.read_parquet('C:/Users/Hubert/Documents/DarlingProject/s2orc_scripts/November_paper/big_ai_dataset_with_affiliations_extended_oa.parquet')

# %%
df.columns = ['paper_id', 'year', 'doi', 'out_citations_count', 'in_citations_count',
       'outbound_citations', 'inbound_citations', 'open_alex', 'institutions',
       'countries', 'types', 'unique_institutions', '%BT',
       'Number of authors', 'double_affiliation', 'company']
# %%
#plot_df = df.iloc[:,[27,28]]
plot_df = df.iloc[:,[15,13]]
plot_df = plot_df.reset_index()
# %%
plt.hist(plot_df['Number of authors'], bins = np.arange(0, 20, 1).tolist())
plt.xlabel('Nr of authors')
plt.ylabel('Nr of papers')
plt.title('Nr of authors per paper distribution')
plt.show()
# %%
for i in range(len(plot_df)):
    if plot_df['Number of authors'][i] >= 10:
         plot_df['Number of authors'][i] = 10
# %%
plt.hist(plot_df['Number of authors'], bins = np.arange(0, 11, 1).tolist())
plt.xlabel('Nr of authors')
plt.ylabel('Nr of papers')
plt.title('Nr of authors per paper distribution')
plt.show()
# %%
plt.hist(plot_df['company'])
plt.xlabel('Nr of authors')
plt.ylabel('Nr of papers')
plt.title('Nr of authors per paper distribution')
plt.show()
# %%
plot_counts = plot_df.groupby(["company", "Number of authors"]).size().reset_index(name="Times")
plot_counts
# %%
for i in range(len(plot_counts)):
    bt = plot_counts['company'][i]
    #bt = plot_counts['%BT'][i]
    if (bt == 0):
        bt = 0
    elif (bt <= 0.05):
        bt = 0.05
    elif (bt <= 0.1):
        bt = 0.1
    elif (bt <= 0.15):
        bt = 0.15
    elif (bt <= 0.2):
        bt = 0.2
    elif (bt <= 0.25):
        bt = 0.25
    elif (bt <= 0.3):
        bt = 0.3
    elif (bt <= 0.35):
        bt = 0.35
    elif (bt <= 0.4):
        bt = 0.4
    elif (bt <= 0.45):
        bt = 0.45
    elif (bt <= 0.5):
        bt = 0.5
    elif (bt <= 0.55):
        bt = 0.55
    elif (bt <= 0.6):
        bt = 0.6
    elif (bt <= 0.65):
        bt = 0.65
    elif (bt <= 0.7):
        bt = 0.7
    elif (bt <= 0.75):
        bt = 0.75
    elif (bt <= 0.8):
        bt = 0.8
    elif (bt <= 0.85):
        bt = 0.85
    elif (bt <= 0.9):
        bt = 0.9
    elif (bt <= 0.95):
        bt = 0.95
    else :
        bt = 1
    #plot_counts['%BT'][i] = bt
    plot_counts['company'][i] = bt

plot_counts
# %%
np.set_printoptions(suppress=True)
tab = np.zeros([21,10])
for i in range(len(plot_counts)):
    #print(plot_counts['%BT'][i] * 20)
    #print(plot_counts['Number of authors'][i])
    tab[int(plot_counts['company'][i] * 20)][int(plot_counts['Number of authors'][i])-1] += plot_counts['Times'][i]
print(tab)
# %%
plot_df = pd.DataFrame(tab)
plot_df.columns = [1,2,3,4,5,6,7,8,9,'10+']
plot_df.index = ['0%', '5%', '10%', '15%', '20%', '25%', '30%', 
'35%', '40%', '45%', '50%', '55%', '60%', '65%', '70%', '75%', 
'80%', '85%', '90%', '95%', '100%']

#%%

# %%
nr_paper_authors = [0] * 10
nr_of_BT_perc = [0] * 21
for i in range(10):
    print()
    nr_paper_authors[i] = plot_df.iloc[:,i].sum()

for i in range(21):
    print()
    nr_of_BT_perc[i] = plot_df.iloc[i].sum()

nr_paper_authors_df = pd.DataFrame({'nr_paper_authors': nr_paper_authors, 'Nr of authors': [1,2,3,4,5,6,7,8,9,'10+']})
nr_of_BT_perc_df = pd.DataFrame({'nr_of_company_perc': nr_of_BT_perc, 'Amount of company authors': ['0%', '5%', '10%', '15%', '20%', '25%', '30%', 
'35%', '40%', '45%', '50%', '55%', '60%', '65%', '70%', '75%', 
'80%', '85%', '90%', '95%', '100%']})
# %%
p2 = sns.barplot(data = nr_paper_authors_df, x = 'Nr of authors', y = nr_paper_authors, color = 'blue')
p2.set_xlabel('')
p2.invert_yaxis()
# %%
p3 = sns.barplot(data = nr_of_BT_perc_df, x = nr_of_BT_perc, y = 'Amount of company authors', color = 'blue')
p3.set_ylabel('')
# %%

# %%
fig = plt.figure(figsize = (15, 15))
ax1 = plt.subplot2grid((3, 3), (0, 0), rowspan=2, colspan=2)
ax2 = plt.subplot2grid((3, 3), (0, 2), rowspan=2, colspan=1)
ax3 = plt.subplot2grid((3, 3), (2, 0), rowspan=1, colspan=2)

p1 = sns.heatmap(plot_df, square = False, norm = LogNorm(), annot = True, 
cmap = 'Blues', fmt = 'g', ax = ax1, cbar = False)
p1.set_xlabel('Number of paper authors', fontsize = 20, fontdict = {'weight': 'bold'})
p1.set_ylabel('Amount of company authors', fontsize = 20, fontdict = {'weight': 'bold'})
p1.set_title('BT involvement in research analysis', fontsize = 30, fontdict = {'weight': 'bold'} )

p2 = sns.barplot(data = nr_paper_authors_df, x = 'Nr of authors', y = nr_paper_authors, color = '#1A68AE', ax = ax3)
p2.set_xlabel('')
p2.invert_yaxis()

p3 = sns.barplot(data = nr_of_BT_perc_df, x = nr_of_BT_perc, y = 'Amount of company authors', color = '#1A68AE', ax = ax2)
p3.set_ylabel('')
p3.set_xscale("log")

plt.tight_layout()
plt.show()

