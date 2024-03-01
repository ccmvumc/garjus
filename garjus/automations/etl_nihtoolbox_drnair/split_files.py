import os
import pandas as pd


data_files = [
    '2023-06-19 #1.csv',
    '2023-10-04 11.34.26 Assessment Data.xls',
    '2024-02-13 12.30.50 Assessment Data - Emily 2.csv',
    '2023-10-04 11.48.57 Assessment Data.xls',
    '2024-02-13 12.46.04 Assessment Data - Devika 3.csv',
    '2023-06-19 10.06.16 Assessment Data.csv',
    '2024-02-13 12.30.13 Assessment Data.csv'
]

score_files = [
    '2023-06-19 #2.csv',
    '2023-06-19 #4.xlsm',
    '2023-06-19 #5.xlsm',
    '2023-10-04 11.34.26 Assessment Scores.xls',
    '2024-02-13 12.30.13 Assessment Scores.csv',
    '2024-02-13 12.46.04 Assessment Scores - Devika2.csv',
    '2023-06-19 10.06.16 Assessment Scores.csv',
    '2022-12-20 chris Assessment Scores.csv',
    '2023-10-04 11.48.57 Assessment Scores.xls',
    '2024-02-13 12.30.50 Assessment Scores - Emily 1.csv',
    '2022-12-20 devika Assessment Scores.csv', 
]

# Load Reg Data
reg1 = pd.read_csv('/home/boydb1/Neurocog Data/2024-02-13 12.30.13 Registration Data.csv', dtype={'PIN': str})
reg1 = reg1.set_index(['PIN', 'Assessment Name'])

reg2 = pd.read_csv('/home/boydb1/Neurocog Data/2024-02-13 12.30.50 Registration Data - Emily3.csv', dtype={'PIN': str})
reg2 = reg2.set_index(['PIN', 'Assessment Name'])

reg3 = pd.read_csv('/home/boydb1/Neurocog Data/2024-02-13 12.46.04 Registration Data - Devika1.csv',  dtype={'PIN': str})
reg3 = reg3.set_index(['PIN', 'Assessment Name'])

reg4 = pd.read_excel('/home/boydb1/Neurocog Data/2023-10-04 11.34.26 Registration Data.xls', dtype={'PIN': str})
reg4 = reg4.set_index(['PIN', 'Assessment Name'])

reg4 = pd.read_excel('/home/boydb1/Neurocog Data/2023-10-04 11.34.26 Registration Data.xls', dtype={'PIN': str})
reg4 = reg4.set_index(['PIN', 'Assessment Name'])

reg5 = pd.read_excel('/home/boydb1/Neurocog Data/2023-10-04 11.48.57 Registration Data.xls', dtype={'PIN': str})
reg5 = reg5.set_index(['PIN', 'Assessment Name'])

reg8 = pd.read_excel('/home/boydb1/Neurocog Data/2023-06-19 #6.xlsm', dtype={'PIN': str})
reg8 = reg8.set_index(['PIN', 'Assessment Name'])

reg9 = pd.read_csv('/home/boydb1/Neurocog Data/2023-06-19 10.06.16 Registration Data.csv', dtype={'PIN': str})
reg9 = reg9.set_index(['PIN', 'Assessment Name'])

# Merge Reg Data
dfr = pd.concat([reg1, reg2, reg3, reg4, reg5, reg8]) #, verify_integrity=True)
dfr = dfr.drop_duplicates()

# Save Reg Data
for i, row in dfr.reset_index().iterrows():
    if not row['PIN'].isnumeric():
        continue

    filename = f'/home/boydb1/Nair_Neurocog/{row["PIN"]}_{row["Assessment Name"]}_Reg.csv'.replace(' ', '')
    print(filename)
    pd.DataFrame([row]).to_csv(filename, index=False)

# Load data files to dfd DataFrame
dfd = pd.DataFrame()
for f in data_files:
    print(f)
    if f.endswith('.csv'):
        reg = pd.read_csv(f'/home/boydb1/Neurocog Data/{f}', dtype={'PIN': str})
    elif f.endswith('.xls'):
        reg = pd.read_excel(f'/home/boydb1/Neurocog Data/{f}', dtype={'PIN': str})

    dfd = pd.concat([dfd, reg])

# Save Data for each PIN/Assessment
visits = dfd[['PIN', 'Assessment Name']].drop_duplicates().sort_values(['PIN', 'Assessment Name'])
for i, v in visits.iterrows():
    if not v['PIN'].isnumeric():
        print(f'not numeric:{i}:{v["PIN"]}')
        continue

    filename = f'/home/boydb1/Nair_Neurocog/{v["PIN"]}_{v["Assessment Name"]}_Data.csv'.replace(' ', '')
    vdfd = dfd[(dfd['PIN'] == v['PIN']) & (dfd['Assessment Name'] == v['Assessment Name'])]
    print(v['PIN'], v['Assessment Name'], len(vdfd), filename)
    pd.DataFrame(vdfd).to_csv(filename, index=False)

# Load score data
dfs = pd.DataFrame()
for f in score_files:
    print(f)
    if f.endswith('.csv'):
        df = pd.read_csv(f'/home/boydb1/Neurocog Data/{f}', dtype={'PIN': str})
    elif f.endswith('.xls'):
        df = pd.read_excel(f'/home/boydb1/Neurocog Data/{f}', dtype={'PIN': str})

    dfs = pd.concat([dfs, df])

# Save score data
score_visits = dfs[['PIN', 'Assessment Name']].drop_duplicates().sort_values(['PIN', 'Assessment Name'])
for i, v in score_visits.iterrows():
    if not v['PIN'].isnumeric():
        continue

    filename = f'/home/boydb1/Nair_Neurocog/{v["PIN"]}_{v["Assessment Name"]}_Score.csv'.replace(' ', '')
    vdfs = dfs[(dfs['PIN'] == v['PIN']) & (dfs['Assessment Name'] == v['Assessment Name'])]
    print(v['PIN'], v['Assessment Name'], len(vdfs), filename)
    pd.DataFrame(vdfs).to_csv(filename, index=False)

print('DONE!')
