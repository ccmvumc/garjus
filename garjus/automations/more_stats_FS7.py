

aparc_columns = [
    'StructName',
    'NumVert',
    'SurfArea',
    'GrayVol',
    'ThickAvg',
    'ThickStd',
    'MeanCurv',
    'GausCurv',
    'FoldInd',
    'CurvInd']


def get_stats(xnat, fullpath):
    stats = {}

    print('downloading lh.aparc')
    xnat_file = xnat.select(fullpath).resource('SUBJ').file('stats/lh.aparc.stats').get()
    df = pd.read_table(
        xnat_file,
        comment='#',
        header=None,
        names=aparc_columns,
        delim_whitespace=True,
        index_col='StructName'
    )

    # Thickness Average
    stats['fs7_caudmidfront_lh_thickavg'] = str(df.loc['caudalmiddlefrontal', 'ThickAvg'])
    stats['fs7_entorhinal_lh_thickavg'] = str(df.loc['entorhinal', 'ThickAvg'])
    stats['fs7_latorbfront_lh_thickavg'] = str(df.loc['lateralorbitofrontal', 'ThickAvg'])
    stats['fs7_medorbfront_lh_thickavg'] = str(df.loc['medialorbitofrontal', 'ThickAvg'])
    stats['fs7_rostmidfront_lh_thickavg'] = str(df.loc['rostralmiddlefrontal', 'ThickAvg'])
    stats['fs7_supfront_lh_thickavg'] = str(df.loc['superiorfrontal', 'ThickAvg'])

    print('downloading rh.aparc')
    xnat_file = xnat.select(fullpath).resource('SUBJ').file('stats/rh.aparc.stats').get()
    df = pd.read_table(
        xnat_file,
        comment='#',
        header=None,
        names=aparc_columns,
        delim_whitespace=True,
        index_col='StructName'
    )

    # Thickness Average
    stats['fs7_caudmidfront_rh_thickavg'] = str(df.loc['caudalmiddlefrontal', 'ThickAvg'])
    stats['fs7_entorhinal_rh_thickavg'] = str(df.loc['entorhinal', 'ThickAvg'])
    stats['fs7_latorbfront_rh_thickavg'] = str(df.loc['lateralorbitofrontal', 'ThickAvg'])
    stats['fs7_medorbfront_rh_thickavg'] = str(df.loc['medialorbitofrontal', 'ThickAvg'])
    stats['fs7_rostmidfront_rh_thickavg'] = str(df.loc['rostralmiddlefrontal', 'ThickAvg'])
    stats['fs7_supfront_rh_thickavg'] = str(df.loc['superiorfrontal', 'ThickAvg'])

    return stats



import pandas as pd
from garjus import Garjus


g = Garjus()

df = g.assessors(projects=['REMBRANDT'])

df = df[df.PROCTYPE == 'FS7_v1']

for i, row in df.iterrows():
    print(i, row['full_path'])

    if i < 1201:
        print('nope')
        continue

    stats = get_stats(g.xnat(), row['full_path'])
    print('set stats')
    g.set_stats(row['PROJECT'], row['SUBJECT'], row['SESSION'], row['ASSR'], stats)

print('DONE!')
