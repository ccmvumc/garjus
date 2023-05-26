
# Left-Nucleus-Accumbens
# Right-Nucleus-Accumbens
# Left-Basal-Forebrain
# Right-Basal-Forebrain
# eTIV
# samseg_lesions
# samseg_sbtiv
# samseg_hpc_lh
# samseg_hpc_rh
# samseg_amg_lh
# samseg_amg_rh
# samseg_nac_lh
# samseg_nac_rh
# fs7_caudmidfront_lh_thickavg
# fs7_entorhinal_lh_thickavg
# fs7_latorbfront_lh_thickavg
# fs7_medorbfront_lh_thickavg
# fs7_rostmidfront_lh_thickavg
# fs7_supfront_lh_thickavg
# fs7_caudmidfront_lh_grayvol
# fs7_entorhinal_lh_grayvol
# fs7_latorbfront_lh_grayvol
# fs7_medorbfront_lh_grayvol
# fs7_rostmidfront_lh_grayvol
# fs7_supfront_lh_grayvol
# fs7_caudmidfront_rh_thickavg
# fs7_entorhinal_rh_thickavg
# fs7_latorbfront_rh_thickavg
# fs7_medorbfront_rh_thickavg
# fs7_rostmidfront_rh_thickavg
# fs7_supfront_rh_thickavg
# fs7_caudmidfront_rh_grayvol
# fs7_entorhinal_rh_grayvol
# fs7_latorbfront_rh_grayvol
# fs7_medorbfront_rh_grayvol
# fs7_rostmidfront_rh_grayvol
# fs7_supfront_rh_grayvol

import pandas as pd

import sys
sys.path.append('/home/boydb1/garjus')

from garjus import Garjus
g = Garjus()

df = g.assessors(projects=['DepMIND2'])
df = df[df.PROCTYPE == 'FS7_v1']


aparc_columns = ['StructName', 'NumVert', 'SurfArea', 'GrayVol', 'ThickAvg', 'ThickStd', 'MeanCurv', 'GausCurv', 'FoldInd', 'CurvInd']

def get_stats(xnat, fullpath):
    stats = {}

    print('downloading lh.aparc')
    xnat_file = xnat.select(fullpath).resource('SUBJ').file('stats/lh.aparc.stats').get()
    df = pd.read_table(xnat_file, comment='#', header=None, names=aparc_columns, delim_whitespace=True, index_col='StructName')

    # Thickness Average
    stats['fs7_caudmidfront_lh_thickavg'] = str(df.loc['caudalmiddlefrontal', 'ThickAvg'])
    stats['fs7_entorhinal_lh_thickavg'] = str(df.loc['entorhinal', 'ThickAvg'])
    stats['fs7_latorbfront_lh_thickavg'] = str(df.loc['lateralorbitofrontal', 'ThickAvg'])
    stats['fs7_medorbfront_lh_thickavg'] = str(df.loc['medialorbitofrontal', 'ThickAvg'])
    stats['fs7_rostmidfront_lh_thickavg'] = str(df.loc['rostralmiddlefrontal', 'ThickAvg'])
    stats['fs7_supfront_lh_thickavg'] = str(df.loc['superiorfrontal', 'ThickAvg'])

    # Gray Matter Volume
    stats['fs7_caudmidfront_lh_grayvol'] = str(df.loc['caudalmiddlefrontal', 'GrayVol'])
    stats['fs7_entorhinal_lh_grayvol'] = str(df.loc['entorhinal', 'GrayVol'])
    stats['fs7_latorbfront_lh_grayvol'] = str(df.loc['lateralorbitofrontal', 'GrayVol'])
    stats['fs7_medorbfront_lh_grayvol'] = str(df.loc['medialorbitofrontal', 'GrayVol'])
    stats['fs7_rostmidfront_lh_grayvol'] = str(df.loc['rostralmiddlefrontal', 'GrayVol'])
    stats['fs7_supfront_lh_grayvol'] = str(df.loc['superiorfrontal', 'GrayVol'])

    print('downloading rh.aparc')
    xnat_file = xnat.select(fullpath).resource('SUBJ').file('stats/rh.aparc.stats').get()
    df = pd.read_table(
        xnat_file, comment='#', header=None, names=aparc_columns, delim_whitespace=True, index_col='StructName')

    # Thickness Average
    stats['fs7_caudmidfront_rh_thickavg'] = str(df.loc['caudalmiddlefrontal', 'ThickAvg'])
    stats['fs7_entorhinal_rh_thickavg'] = str(df.loc['entorhinal', 'ThickAvg'])
    stats['fs7_latorbfront_rh_thickavg'] = str(df.loc['lateralorbitofrontal', 'ThickAvg'])
    stats['fs7_medorbfront_rh_thickavg'] = str(df.loc['medialorbitofrontal', 'ThickAvg'])
    stats['fs7_rostmidfront_rh_thickavg'] = str(df.loc['rostralmiddlefrontal', 'ThickAvg'])
    stats['fs7_supfront_rh_thickavg'] = str(df.loc['superiorfrontal', 'ThickAvg'])

    # Gray Matter Volume
    stats['fs7_caudmidfront_rh_grayvol'] = str(df.loc['caudalmiddlefrontal', 'GrayVol'])
    stats['fs7_entorhinal_rh_grayvol'] = str(df.loc['entorhinal', 'GrayVol'])
    stats['fs7_latorbfront_rh_grayvol'] = str(df.loc['lateralorbitofrontal', 'GrayVol'])
    stats['fs7_medorbfront_rh_grayvol'] = str(df.loc['medialorbitofrontal', 'GrayVol'])
    stats['fs7_rostmidfront_rh_grayvol'] = str(df.loc['rostralmiddlefrontal', 'GrayVol'])
    stats['fs7_supfront_rh_grayvol'] = str(df.loc['superiorfrontal', 'GrayVol'])

    return stats


for i, row in df.iterrows():
    print(i, row['full_path'])
    stats = get_stats(g.xnat(), row['full_path'])
    print('set stats')
    g.set_stats(row['PROJECT'], row['SUBJECT'], row['SESSION'], row['ASSR'], stats)



samseg_columns = ['Name', 'Volume', 'Units']

def get_samseg_stats(xnat, fullpath):
    stats = {}

    print('downloading samseg.stats')
    xnat_file = xnat.select(fullpath).resource('DATA').file('samseg.stats').get()
    df = pd.read_table(xnat_file, header=None, names=samseg_columns, sep=',', index_col='Name')

    stats['samseg_hpc_lh'] = str(df.loc['# Measure Left-Hippocampus', 'Volume'])
    stats['samseg_hpc_rh'] = str(df.loc['# Measure Right-Hippocampus', 'Volume'])
    stats['samseg_amg_lh'] = str(df.loc['# Measure Left-Amygdala', 'Volume'])
    stats['samseg_amg_rh'] = str(df.loc['# Measure Right-Amygdala', 'Volume'])
    stats['samseg_nac_lh'] = str(df.loc['# Measure Left-Accumbens-area', 'Volume'])
    stats['samseg_nac_rh'] = str(df.loc['# Measure Right-Accumbens-area', 'Volume'])

    return stats

df2 = g.assessors(projects=['DepMIND2'], proctypes=['SAMSEG_v1'])
for i, row in df2.iterrows():
    print(i, row['full_path'])
    stats = get_samseg_stats(g.xnat(), row['full_path'])
    print('set stats')
    print(stats)
    g.set_stats(row['PROJECT'], row['SUBJECT'], row['SESSION'], row['ASSR'], stats)

