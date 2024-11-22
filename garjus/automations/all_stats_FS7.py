import pandas as pd

from garjus import Garjus


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
    'CurvInd'
]

aseg_columns = [
    'Index',
    'SegId',
    'NVoxels',
    'Volume_mm3',
    'StructName',
    'normMean',
    'normStdDev',
    'normMin',
    'normMax',
    'normRange'
]


def get_stats(xnat, fullpath):
    stats = {}

    print('downloading lh.aparc')
    xnat_file = xnat.select(fullpath).resource('SUBJ').file('stats/lh.aparc.stats').get()
    df = pd.read_table(
        xnat_file,
        comment='#',
        header=None,
        names=aparc_columns,
        sep='\s+',
        index_col='StructName'
    )

    # Left Hemisphere Thickness
    stats['caudalmiddlefrontal_lh_thickavg'] = str(df.loc['caudalmiddlefrontal', 'ThickAvg'])
    stats['entorhinal_lh_thickavg'] = str(df.loc['entorhinal', 'ThickAvg'])
    stats['lateralorbitofrontal_lh_thickavg'] = str(df.loc['lateralorbitofrontal', 'ThickAvg'])
    stats['medialorbitofrontal_lh_thickavg'] = str(df.loc['medialorbitofrontal', 'ThickAvg'])
    stats['rostralmiddlefrontal_lh_thickavg'] = str(df.loc['rostralmiddlefrontal', 'ThickAvg'])
    stats['superiorfrontal_lh_thickavg'] = str(df.loc['superiorfrontal', 'ThickAvg'])
    stats['bankssts_lh_thickavg'] = str(df.loc['bankssts', 'ThickAvg'])
    stats['caudalanteriorcingulate_lh_thickavg'] = str(df.loc['caudalanteriorcingulate', 'ThickAvg'])
    stats['cuneus_lh_thickavg'] = str(df.loc['cuneus', 'ThickAvg'])
    stats['frontalpole_lh_thickavg'] = str(df.loc['frontalpole', 'ThickAvg'])
    stats['fusiform_lh_thickavg'] = str(df.loc['fusiform', 'ThickAvg'])
    stats['inferiorparietal_lh_thickavg'] = str(df.loc['inferiorparietal', 'ThickAvg'])
    stats['inferiortemporal_lh_thickavg'] = str(df.loc['inferiortemporal', 'ThickAvg'])
    stats['insula_lh_thickavg'] = str(df.loc['insula', 'ThickAvg'])
    stats['isthmuscingulate_lh_thickavg'] = str(df.loc['isthmuscingulate', 'ThickAvg'])
    stats['lateraloccipital_lh_thickavg'] = str(df.loc['lateraloccipital', 'ThickAvg'])
    stats['lingual_lh_thickavg'] = str(df.loc['lingual', 'ThickAvg'])
    stats['middletemporal_lh_thickavg'] = str(df.loc['middletemporal', 'ThickAvg'])
    stats['parahippocampal_lh_thickavg'] = str(df.loc['parahippocampal', 'ThickAvg'])
    stats['paracentral_lh_thickavg'] = str(df.loc['paracentral', 'ThickAvg'])
    stats['parsopercularis_lh_thickavg'] = str(df.loc['parsopercularis', 'ThickAvg'])
    stats['parsorbitalis_lh_thickavg'] = str(df.loc['parsorbitalis', 'ThickAvg'])
    stats['parstriangularis_lh_thickavg'] = str(df.loc['parstriangularis', 'ThickAvg'])
    stats['pericalcarine_lh_thickavg'] = str(df.loc['pericalcarine', 'ThickAvg'])   
    stats['postcentral_lh_thickavg'] = str(df.loc['postcentral', 'ThickAvg'])
    stats['posteriorcingulate_lh_thickavg'] = str(df.loc['posteriorcingulate', 'ThickAvg'])    
    stats['precentral_lh_thickavg'] = str(df.loc['precentral', 'ThickAvg'])
    stats['precuneus_lh_thickavg'] = str(df.loc['precuneus', 'ThickAvg'])
    stats['rostralanteriorcingulate_lh_thickavg'] = str(df.loc['rostralanteriorcingulate', 'ThickAvg'])    
    stats['superiorparietal_lh_thickavg'] = str(df.loc['superiorparietal', 'ThickAvg'])
    stats['superiortemporal_lh_thickavg'] = str(df.loc['superiortemporal', 'ThickAvg'])
    stats['supramarginal_lh_thickavg'] = str(df.loc['supramarginal', 'ThickAvg'])
    stats['temporalpole_lh_thickavg'] = str(df.loc['temporalpole', 'ThickAvg'])
    stats['transversetemporal_lh_thickavg'] = str(df.loc['transversetemporal', 'ThickAvg'])

    # Left Hemisphere volumes
    stats['caudalmiddlefrontal_lh_volume'] = str(df.loc['caudalmiddlefrontal', 'GrayVol'])
    stats['entorhinal_lh_volume'] = str(df.loc['entorhinal', 'GrayVol'])
    stats['lateralorbitofrontal_lh_volume'] = str(df.loc['lateralorbitofrontal', 'GrayVol'])
    stats['medialorbitofrontal_lh_volume'] = str(df.loc['medialorbitofrontal', 'GrayVol'])
    stats['rostralmiddlefrontal_lh_volume'] = str(df.loc['rostralmiddlefrontal', 'GrayVol'])
    stats['superiorfrontal_lh_volume'] = str(df.loc['superiorfrontal', 'GrayVol'])
    stats['bankssts_lh_volume'] = str(df.loc['bankssts', 'GrayVol'])
    stats['caudalanteriorcingulate_lh_volume'] = str(df.loc['caudalanteriorcingulate', 'GrayVol'])
    stats['cuneus_lh_volume'] = str(df.loc['cuneus', 'GrayVol'])
    stats['frontalpole_lh_volume'] = str(df.loc['frontalpole', 'GrayVol'])
    stats['fusiform_lh_volume'] = str(df.loc['fusiform', 'GrayVol'])
    stats['inferiorparietal_lh_volume'] = str(df.loc['inferiorparietal', 'GrayVol'])
    stats['inferiortemporal_lh_volume'] = str(df.loc['inferiortemporal', 'GrayVol'])
    stats['insula_lh_volume'] = str(df.loc['insula', 'GrayVol'])
    stats['isthmuscingulate_lh_volume'] = str(df.loc['isthmuscingulate', 'GrayVol'])
    stats['lateraloccipital_lh_volume'] = str(df.loc['lateraloccipital', 'GrayVol'])
    stats['lingual_lh_volume'] = str(df.loc['lingual', 'GrayVol'])
    stats['middletemporal_lh_volume'] = str(df.loc['middletemporal', 'GrayVol'])
    stats['parahippocampal_lh_volume'] = str(df.loc['parahippocampal', 'GrayVol'])
    stats['paracentral_lh_volume'] = str(df.loc['paracentral', 'GrayVol'])
    stats['parsopercularis_lh_volume'] = str(df.loc['parsopercularis', 'GrayVol'])
    stats['parsorbitalis_lh_volume'] = str(df.loc['parsorbitalis', 'GrayVol'])
    stats['parstriangularis_lh_volume'] = str(df.loc['parstriangularis', 'GrayVol'])
    stats['pericalcarine_lh_volume'] = str(df.loc['pericalcarine', 'GrayVol'])   
    stats['postcentral_lh_volume'] = str(df.loc['postcentral', 'GrayVol'])
    stats['posteriorcingulate_lh_volume'] = str(df.loc['posteriorcingulate', 'GrayVol'])    
    stats['precentral_lh_volume'] = str(df.loc['precentral', 'GrayVol'])
    stats['precuneus_lh_volume'] = str(df.loc['precuneus', 'GrayVol'])
    stats['rostralanteriorcingulate_lh_volume'] = str(df.loc['rostralanteriorcingulate', 'GrayVol'])    
    stats['superiorparietal_lh_volume'] = str(df.loc['superiorparietal', 'GrayVol'])
    stats['superiortemporal_lh_volume'] = str(df.loc['superiortemporal', 'GrayVol'])
    stats['supramarginal_lh_volume'] = str(df.loc['supramarginal', 'GrayVol'])
    stats['temporalpole_lh_volume'] = str(df.loc['temporalpole', 'GrayVol'])
    stats['transversetemporal_lh_volume'] = str(df.loc['transversetemporal', 'GrayVol'])

    # Left Hemisphere Surface Area
    stats['caudalmiddlefrontal_lh_surfarea'] = str(df.loc['caudalmiddlefrontal', 'SurfArea'])
    stats['entorhinal_lh_surfarea'] = str(df.loc['entorhinal', 'SurfArea'])
    stats['lateralorbitofrontal_lh_surfarea'] = str(df.loc['lateralorbitofrontal', 'SurfArea'])
    stats['medialorbitofrontal_lh_surfarea'] = str(df.loc['medialorbitofrontal', 'SurfArea'])
    stats['rostralmiddlefront_lh_surfarea'] = str(df.loc['rostralmiddlefrontal', 'SurfArea'])
    stats['superiorfrontal_lh_surfarea'] = str(df.loc['superiorfrontal', 'SurfArea'])
    stats['bankssts_lh_surfarea'] = str(df.loc['bankssts', 'SurfArea'])
    stats['caudalanteriorcingulate_lh_surfarea'] = str(df.loc['caudalanteriorcingulate', 'SurfArea'])
    stats['cuneus_lh_surfarea'] = str(df.loc['cuneus', 'SurfArea'])
    stats['frontalpole_lh_surfarea'] = str(df.loc['frontalpole', 'SurfArea'])
    stats['fusiform_lh_surfarea'] = str(df.loc['fusiform', 'SurfArea'])
    stats['inferiorparietal_lh_surfarea'] = str(df.loc['inferiorparietal', 'SurfArea'])
    stats['inferiortemporal_lh_surfarea'] = str(df.loc['inferiortemporal', 'SurfArea'])
    stats['insula_lh_surfarea'] = str(df.loc['insula', 'SurfArea'])
    stats['isthmuscingulate_lh_surfarea'] = str(df.loc['isthmuscingulate', 'SurfArea'])
    stats['lateraloccipital_lh_surfarea'] = str(df.loc['lateraloccipital', 'SurfArea'])
    stats['lingual_lh_surfarea'] = str(df.loc['lingual', 'SurfArea'])
    stats['middletemporal_lh_surfarea'] = str(df.loc['middletemporal', 'SurfArea'])
    stats['parahippocampal_lh_surfarea'] = str(df.loc['parahippocampal', 'SurfArea'])
    stats['paracentral_lh_surfarea'] = str(df.loc['paracentral', 'SurfArea'])
    stats['parsopercularis_lh_surfarea'] = str(df.loc['parsopercularis', 'SurfArea'])
    stats['parsorbitalis_lh_surfarea'] = str(df.loc['parsorbitalis', 'SurfArea'])
    stats['parstriangularis_lh_surfarea'] = str(df.loc['parstriangularis', 'SurfArea'])
    stats['pericalcarine_lh_surfarea'] = str(df.loc['pericalcarine', 'SurfArea'])   
    stats['postcentral_lh_surfarea'] = str(df.loc['postcentral', 'SurfArea'])
    stats['posteriorcingulate_lh_surfarea'] = str(df.loc['posteriorcingulate', 'SurfArea'])    
    stats['precentral_lh_surfarea'] = str(df.loc['precentral', 'SurfArea'])
    stats['precuneus_lh_surfarea'] = str(df.loc['precuneus', 'SurfArea'])
    stats['rostralanteriorcingulate_lh_surfarea'] = str(df.loc['rostralanteriorcingulate', 'SurfArea'])    
    stats['superiorparietal_lh_surfarea'] = str(df.loc['superiorparietal', 'SurfArea'])
    stats['superiortemporal_lh_surfarea'] = str(df.loc['superiortemporal', 'SurfArea'])
    stats['supramarginal_lh_surfarea'] = str(df.loc['supramarginal', 'SurfArea'])
    stats['temporalpole_lh_surfarea'] = str(df.loc['temporalpole', 'SurfArea'])
    stats['transversetemporal_lh_surfarea'] = str(df.loc['transversetemporal', 'SurfArea'])

    print('downloading rh.aparc')
    xnat_file = xnat.select(fullpath).resource('SUBJ').file('stats/rh.aparc.stats').get()
    df = pd.read_table(
        xnat_file,
        comment='#',
        header=None,
        names=aparc_columns,
        sep='\s+',
        index_col='StructName'
    )

    # Right Hemisphere Thickness Average
    stats['caudalmiddlefrontal_rh_thickavg'] = str(df.loc['caudalmiddlefrontal', 'ThickAvg'])
    stats['entorhinal_rh_thickavg'] = str(df.loc['entorhinal', 'ThickAvg'])
    stats['lateralorbitofrontal_rh_thickavg'] = str(df.loc['lateralorbitofrontal', 'ThickAvg'])
    stats['medialorbitofrontal_rh_thickavg'] = str(df.loc['medialorbitofrontal', 'ThickAvg'])
    stats['rostralmiddlefrontal_rh_thickavg'] = str(df.loc['rostralmiddlefrontal', 'ThickAvg'])
    stats['superiorfrontal_rh_thickavg'] = str(df.loc['superiorfrontal', 'ThickAvg'])
    stats['bankssts_rh_thickavg'] = str(df.loc['bankssts', 'ThickAvg'])
    stats['caudalanteriorcingulate_rh_thickavg'] = str(df.loc['caudalanteriorcingulate', 'ThickAvg'])
    stats['cuneus_rh_thickavg'] = str(df.loc['cuneus', 'ThickAvg'])
    stats['frontalpole_rh_thickavg'] = str(df.loc['frontalpole', 'ThickAvg'])
    stats['fusiform_rh_thickavg'] = str(df.loc['fusiform', 'ThickAvg'])
    stats['inferiorparietal_rh_thickavg'] = str(df.loc['inferiorparietal', 'ThickAvg'])
    stats['inferiortemporal_rh_thickavg'] = str(df.loc['inferiortemporal', 'ThickAvg'])
    stats['insula_rh_thickavg'] = str(df.loc['insula', 'ThickAvg'])
    stats['isthmuscingulate_rh_thickavg'] = str(df.loc['isthmuscingulate', 'ThickAvg'])
    stats['lateraloccipital_rh_thickavg'] = str(df.loc['lateraloccipital', 'ThickAvg'])
    stats['lingual_rh_thickavg'] = str(df.loc['lingual', 'ThickAvg'])
    stats['middletemporal_rh_thickavg'] = str(df.loc['middletemporal', 'ThickAvg'])
    stats['parahippocampal_rh_thickavg'] = str(df.loc['parahippocampal', 'ThickAvg'])
    stats['paracentral_rh_thickavg'] = str(df.loc['paracentral', 'ThickAvg'])
    stats['parsopercularis_rh_thickavg'] = str(df.loc['parsopercularis', 'ThickAvg'])
    stats['parsorbitalis_rh_thickavg'] = str(df.loc['parsorbitalis', 'ThickAvg'])
    stats['parstriangularis_rh_thickavg'] = str(df.loc['parstriangularis', 'ThickAvg'])
    stats['pericalcarine_rh_thickavg'] = str(df.loc['pericalcarine', 'ThickAvg'])   
    stats['postcentral_rh_thickavg'] = str(df.loc['postcentral', 'ThickAvg'])
    stats['posteriorcingulate_rh_thickavg'] = str(df.loc['posteriorcingulate', 'ThickAvg'])    
    stats['precentral_rh_thickavg'] = str(df.loc['precentral', 'ThickAvg'])
    stats['precuneus_rh_thickavg'] = str(df.loc['precuneus', 'ThickAvg'])
    stats['rostralanteriorcingulate_rh_thickavg'] = str(df.loc['rostralanteriorcingulate', 'ThickAvg'])    
    stats['superiorparietal_rh_thickavg'] = str(df.loc['superiorparietal', 'ThickAvg'])
    stats['superiortemporal_rh_thickavg'] = str(df.loc['superiortemporal', 'ThickAvg'])
    stats['supramarginal_rh_thickavg'] = str(df.loc['supramarginal', 'ThickAvg'])
    stats['temporalpole_rh_thickavg'] = str(df.loc['temporalpole', 'ThickAvg'])
    stats['transversetemporal_rh_thickavg'] = str(df.loc['transversetemporal', 'ThickAvg'])

    # Right Hemisphere Volumes
    stats['caudalmiddlefrontal_rh_volume'] = str(df.loc['caudalmiddlefrontal', 'GrayVol'])
    stats['entorhinal_rh_volume'] = str(df.loc['entorhinal', 'GrayVol'])
    stats['lateralorbitofrontal_rh_volume'] = str(df.loc['lateralorbitofrontal', 'GrayVol'])
    stats['medialorbitofrontal_rh_volume'] = str(df.loc['medialorbitofrontal', 'GrayVol'])
    stats['rostralmiddlefrontal_rh_volume'] = str(df.loc['rostralmiddlefrontal', 'GrayVol'])
    stats['superiorfrontal_rh_volume'] = str(df.loc['superiorfrontal', 'GrayVol'])
    stats['bankssts_rh_volume'] = str(df.loc['bankssts', 'GrayVol'])
    stats['caudalanteriorcingulate_rh_volume'] = str(df.loc['caudalanteriorcingulate', 'GrayVol'])
    stats['cuneus_rh_volume'] = str(df.loc['cuneus', 'GrayVol'])
    stats['frontalpole_rh_volume'] = str(df.loc['frontalpole', 'GrayVol'])
    stats['fusiform_rh_volume'] = str(df.loc['fusiform', 'GrayVol'])
    stats['inferiorparietal_rh_volume'] = str(df.loc['inferiorparietal', 'GrayVol'])
    stats['inferiortemporal_rh_volume'] = str(df.loc['inferiortemporal', 'GrayVol'])
    stats['insula_rh_volume'] = str(df.loc['insula', 'GrayVol'])
    stats['isthmuscingulate_rh_volume'] = str(df.loc['isthmuscingulate', 'GrayVol'])
    stats['lateraloccipital_rh_volume'] = str(df.loc['lateraloccipital', 'GrayVol'])
    stats['lingual_rh_volume'] = str(df.loc['lingual', 'GrayVol'])
    stats['middletemporal_rh_volume'] = str(df.loc['middletemporal', 'GrayVol'])
    stats['parahippocampal_rh_volume'] = str(df.loc['parahippocampal', 'GrayVol'])
    stats['paracentral_rh_volume'] = str(df.loc['paracentral', 'GrayVol'])
    stats['parsopercularis_rh_volume'] = str(df.loc['parsopercularis', 'GrayVol'])
    stats['parsorbitalis_rh_volume'] = str(df.loc['parsorbitalis', 'GrayVol'])
    stats['parstriangularis_rh_volume'] = str(df.loc['parstriangularis', 'GrayVol'])
    stats['pericalcarine_rh_volume'] = str(df.loc['pericalcarine', 'GrayVol'])   
    stats['postcentral_rh_volume'] = str(df.loc['postcentral', 'GrayVol'])
    stats['posteriorcingulate_rh_volume'] = str(df.loc['posteriorcingulate', 'GrayVol'])    
    stats['precentral_rh_volume'] = str(df.loc['precentral', 'GrayVol'])
    stats['precuneus_rh_volume'] = str(df.loc['precuneus', 'GrayVol'])
    stats['rostralanteriorcingulate_rh_volume'] = str(df.loc['rostralanteriorcingulate', 'GrayVol'])    
    stats['superiorparietal_rh_volume'] = str(df.loc['superiorparietal', 'GrayVol'])
    stats['superiortemporal_rh_volume'] = str(df.loc['superiortemporal', 'GrayVol'])
    stats['supramarginal_rh_volume'] = str(df.loc['supramarginal', 'GrayVol'])
    stats['temporalpole_rh_volume'] = str(df.loc['temporalpole', 'GrayVol'])
    stats['transversetemporal_rh_volume'] = str(df.loc['transversetemporal', 'GrayVol'])

    # Right Hemisphere Surface Area
    stats['caudalmiddlefrontal_rh_surfarea'] = str(df.loc['caudalmiddlefrontal', 'SurfArea'])
    stats['entorhinal_rh_surfarea'] = str(df.loc['entorhinal', 'SurfArea'])
    stats['lateralorbitofrontal_rh_surfarea'] = str(df.loc['lateralorbitofrontal', 'SurfArea'])
    stats['medialorbitofrontal_rh_surfarea'] = str(df.loc['medialorbitofrontal', 'SurfArea'])
    stats['rostralmiddlefront_rh_surfarea'] = str(df.loc['rostralmiddlefrontal', 'SurfArea'])
    stats['superiorfrontal_rh_surfarea'] = str(df.loc['superiorfrontal', 'SurfArea'])
    stats['bankssts_rh_surfarea'] = str(df.loc['bankssts', 'SurfArea'])
    stats['caudalanteriorcingulate_rh_surfarea'] = str(df.loc['caudalanteriorcingulate', 'SurfArea'])
    stats['cuneus_rh_surfarea'] = str(df.loc['cuneus', 'SurfArea'])
    stats['frontalpole_rh_surfarea'] = str(df.loc['frontalpole', 'SurfArea'])
    stats['fusiform_rh_surfarea'] = str(df.loc['fusiform', 'SurfArea'])
    stats['inferiorparietal_rh_surfarea'] = str(df.loc['inferiorparietal', 'SurfArea'])
    stats['inferiortemporal_rh_surfarea'] = str(df.loc['inferiortemporal', 'SurfArea'])
    stats['insula_rh_surfarea'] = str(df.loc['insula', 'SurfArea'])
    stats['isthmuscingulate_rh_surfarea'] = str(df.loc['isthmuscingulate', 'SurfArea'])
    stats['lateraloccipital_rh_surfarea'] = str(df.loc['lateraloccipital', 'SurfArea'])
    stats['lingual_rh_surfarea'] = str(df.loc['lingual', 'SurfArea'])
    stats['middletemporal_rh_surfarea'] = str(df.loc['middletemporal', 'SurfArea'])
    stats['parahippocampal_rh_surfarea'] = str(df.loc['parahippocampal', 'SurfArea'])
    stats['paracentral_rh_surfarea'] = str(df.loc['paracentral', 'SurfArea'])
    stats['parsopercularis_rh_surfarea'] = str(df.loc['parsopercularis', 'SurfArea'])
    stats['parsorbitalis_rh_surfarea'] = str(df.loc['parsorbitalis', 'SurfArea'])
    stats['parstriangularis_rh_surfarea'] = str(df.loc['parstriangularis', 'SurfArea'])
    stats['pericalcarine_rh_surfarea'] = str(df.loc['pericalcarine', 'SurfArea'])   
    stats['postcentral_rh_surfarea'] = str(df.loc['postcentral', 'SurfArea'])
    stats['posteriorcingulate_rh_surfarea'] = str(df.loc['posteriorcingulate', 'SurfArea'])    
    stats['precentral_rh_surfarea'] = str(df.loc['precentral', 'SurfArea'])
    stats['precuneus_rh_surfarea'] = str(df.loc['precuneus', 'SurfArea'])
    stats['rostralanteriorcingulate_rh_surfarea'] = str(df.loc['rostralanteriorcingulate', 'SurfArea'])    
    stats['superiorparietal_rh_surfarea'] = str(df.loc['superiorparietal', 'SurfArea'])
    stats['superiortemporal_rh_surfarea'] = str(df.loc['superiortemporal', 'SurfArea'])
    stats['supramarginal_rh_surfarea'] = str(df.loc['supramarginal', 'SurfArea'])
    stats['temporalpole_rh_surfarea'] = str(df.loc['temporalpole', 'SurfArea'])
    stats['transversetemporal_rh_surfarea'] = str(df.loc['transversetemporal', 'SurfArea'])

    print('downloading aseg')
    xnat_file = xnat.select(fullpath).resource('SUBJ').file('stats/aseg.stats').get()
    df = pd.read_table(
        xnat_file,
        comment='#',
        header=None,
        names=aseg_columns,
        sep='\s+',
        index_col='StructName'
    )

    # Subcortical Volumes Left
    stats['accumbensarea_lh_volume'] = str(df.loc['Left-Accumbens-area', 'Volume_mm3'])
    stats['amygdala_lh_volume'] = str(df.loc['Left-Amygdala', 'Volume_mm3'])
    stats['caudate_lh_volume'] = str(df.loc['Left-Caudate', 'Volume_mm3'])
    stats['hippocampus_lh_volume'] = str(df.loc['Left-Hippocampus', 'Volume_mm3'])
    stats['pallidum_lh_volume'] = str(df.loc['Left-Pallidum', 'Volume_mm3'])
    stats['putamen_lh_volume'] = str(df.loc['Left-Putamen', 'Volume_mm3'])
    stats['thalamus_lh_volume'] = str(df.loc['Left-Thalamus', 'Volume_mm3'])
    stats['ventraldc_lh_volume'] = str(df.loc['Left-VentralDC', 'Volume_mm3'])

    # Subcortical Volumes Right
    stats['accumbensarea_rh_volume'] = str(df.loc['Right-Accumbens-area', 'Volume_mm3'])
    stats['amygdala_rh_volume'] = str(df.loc['Right-Amygdala', 'Volume_mm3'])
    stats['caudate_rh_volume'] = str(df.loc['Right-Caudate', 'Volume_mm3'])
    stats['hippocampus_rh_volume'] = str(df.loc['Right-Hippocampus', 'Volume_mm3'])
    stats['pallidum_rh_volume'] = str(df.loc['Right-Pallidum', 'Volume_mm3'])
    stats['putamen_rh_volume'] = str(df.loc['Right-Putamen', 'Volume_mm3'])
    stats['thalamus_rh_volume'] = str(df.loc['Right-Thalamus', 'Volume_mm3'])
    stats['ventraldc_rh_volume'] = str(df.loc['Right-VentralDC', 'Volume_mm3'])

    # Hypointensities
    stats['wmh_volume'] = str(df.loc['WM-hypointensities', 'Volume_mm3'])

    return stats



g = Garjus()

df = g.assessors(projects=['REMBRANDT'])

df = df[df.PROCTYPE == 'FS7_v1']

df = df.sort_values('ASSR')

for i, row in df.iterrows():
    print(i, row['full_path'])
    stats = get_stats(g.xnat(), row['full_path'])
    print('set stats')
    g.set_stats(row['PROJECT'], row['SUBJECT'], row['SESSION'], row['ASSR'], stats)

print('DONE!')
