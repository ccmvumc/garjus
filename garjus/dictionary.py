ACTIVITY_RENAME = {
    'redcap_repeat_instance': 'ID',
    'activity_description': 'DESCRIPTION',
    'activity_datetime': 'DATETIME',
    'activity_event': 'EVENT',
    'activity_repeat': 'REPEAT',
    'activity_field': 'FIELD',
    'activity_result': 'RESULT',
    'activity_scan': 'SCAN',
    'activity_subject': 'SUBJECT',
    'activity_session': 'SESSION',
    'activity_type': 'CATEGORY',
}

ANALYSES_RENAME = {
    'redcap_repeat_instance': 'ID',
    'analysis_name': 'NAME',
    'analysis_lead': 'INVESTIGATOR',
    'analysis_include': 'SUBJECTS',
    'analysis_processor': 'PROCESSOR',
    'analysis_input': 'INPUT',
    'analysis_output': 'OUTPUT',
    'analyses_complete': 'COMPLETE',
    'analysis_status': 'STATUS',
    'analysis_covars': 'COVARS',
    'analysis_notes': 'NOTES',
}

ISSUES_RENAME = {
    'redcap_repeat_instance': 'ID',
    'issue_date': 'DATETIME',
    'issue_description': 'DESCRIPTION',
    'issue_event': 'EVENT',
    'issue_field': 'FIELD',
    'issue_scan': 'SCAN',
    'issue_session': 'SESSION',
    'issue_subject': 'SUBJECT',
    'issue_type': 'CATEGORY',
}

PROCESSING_RENAME = {
    'redcap_repeat_instance': 'ID',
    'processor_file': 'FILE',
    'processor_filter': 'FILTER',
    'processor_args': 'ARGS',
    'processing_complete': 'COMPLETE',
}

TASKS_RENAME = {
    'task_assessor': 'ASSESSOR',
    'task_status': 'STATUS',
    'task_inputlist': 'INPUTLIST',
    'task_var2val': 'VAR2VAL',
    'task_memreq': 'MEMREQ',
    'task_walltime': 'WALLTIME',
    'task_procdate': 'PROCDATE',
    'task_timeused': 'TIMEUSED',
    'task_memused': 'MEMUSED',
    'task_yamlfile': 'YAMLFILE',
    'task_userinputs': 'USERINPUTS',
    'task_failcount': 'FAILCOUNT',
    'task_yamlupload': 'YAMLUPLOAD',
}

REPORTS_RENAME = {
    'redcap_repeat_instance': 'ID',
    'progress_name': 'NAME',
    'progress_datetime': 'DATE',
    'progress_pdf': 'PDF',
    'progress_zip': 'DATA',
    'double_resultspdf': 'PDF',
    'double_resultsfile': 'DATA',
    'double_name': 'NAME',
    'double_datetime': 'DATE',
    'ndaimage03_name': 'NAME',
    'ndaimage03_csvfile': 'DATA',
}

DISABLE_STATTYPES = [
    'fmri_rest_v2',
    'fmri_rest_v4',
    'fmri_rest_v5',
    'fmri_roi_v1',
    'struct_preproc_noflair_v1',
    'fmri_nback_v2',
    'fmri_roi_v2',
    'NMQA_v1',
    'francois_schaefer200_v1',
    'francois_schaefer400_v1',
    'dtiQA_synb0_v7',
    'EEG-EGI-wavelet_v1',
    'EEG-EGI-preproc_v2',
    'synbold_v1',
    'fmri_rest_schaef_conn_v1',
    'FS7thalamus_v1',
]

COLUMNS = {
    'activity': [
        'PROJECT', 'SUBJECT', 'SESSION', 'SCAN', 'ID', 'DESCRIPTION',
        'DATETIME', 'EVENT', 'REPEAT', 'FIELD', 'CATEGORY', 'RESULT', 'STATUS', 'SOURCE'],
    'assessors': [
        'PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'NOTE', 'DATE', 'SITE',
        'ASSR', 'PROCSTATUS', 'PROCTYPE', 'JOBDATE', 'TIMEUSED', 'MEMUSED',
        'QCSTATUS', 'QCDATE', 'QCBY', 'XSITYPE', 'INPUTS', 'MODALITY', 'full_path'],
    'issues': [
        'ID', 'DATETIME', 'PROJECT', 'CATEGORY',
        'SUBJECT', 'SESSION', 'SCAN ', 'DESCRIPTION',
        'EVENT', 'FIELD', 'STATUS'],
    'scans': [
        'PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'TRACER', 'NOTE', 'DATE', 'SITE',
        'DURATION', 'FRAMES', 'TR', 'THICK', 'SENSE', 'MB',
        'SCANID', 'SCANTYPE', 'QUALITY', 'RESOURCES', 'MODALITY', 'XSITYPE', 'full_path'],
    'processing': [
        'ID', 'PROJECT', 'TYPE', 'FILTER', 'FILE', 'CUSTOM', 'ARGS', 'YAMLUPLOAD', 'EDIT', 'COMPLETE'],
    'subjects': [
        'PROJECT', 'SUBJECT', 'AGE', 'SEX', 'RACE'],
    'tasks': [
        'ID', 'IDLINK', 'PROJECT', 'STATUS', 'PROCTYPE', 'MEMREQ', 'WALLTIME',
        'TIMEUSED', 'MEMUSED', 'ASSESSOR', 'PROCDATE', 'INPUTLIST', 'VAR2VAL',
        'IMAGEDIR', 'JOBTEMPLATE', 'YAMLFILE', 'YAMLUPLOAD', 'USERINPUTS', 'FAILCOUNT'],
    'analyses': ['PROJECT', 'ID', 'NAME', 'STATUS', 'EDIT', 'NOTES', 'SUBJECTS', 'PROCESSOR', 'INVESTIGATOR', 'OUTPUT'],
    'processors': ['ID', 'PROJECT', 'TYPE', 'EDIT', 'FILE', 'FILTER', 'ARGS'],
    'sgp': ['PROJECT', 'SUBJECT', 'ASSR', 'PROCSTATUS', 'PROCTYPE', 'QCSTATUS', 'INPUTS', 'DATE', 'XSITYPE'],
    'reports': ['PROJECT', 'TYPE', 'ID', 'VIEW', 'NAME', 'DATE', 'PDF', 'DATA'],
}


PROCLIB = {
    'AMYVIDQA_v2': {
        'short_descrip': 'Regional Amyloid SUVR using cerebellum as reference.',
        'inputs_descrip': 'T1w MRI processed with FreeSurfer (FS7_v1), Amyloid PET',
        'procurl': 'https://github.com/ccmvumc/AMYVIDQA',
        'stats_subset': ['compositegm_suvr', 'cblmtot_suvr', 'cblmwm_suvr', 'cblmgm_suvr', 'hippocampus_suvr']
    },
    'BFC_v2': {
        'short_descrip': 'Basal Forebrain Volumes.',
        'inputs_descrip': 'T1w MRI',
        'procurl': 'https://github.com/ccmvumc/BFC',
        'stats_subset': ['CH4_L_VOL', 'CH4_R_VOL']
    },
    'BrainAgeGap_v2': {
        'short_descrip': 'Predicted age of brain.',
        'inputs_descrip': 'T1w MRI parcellated with BrainColor atlas',
        'procurl': 'https://pubmed.ncbi.nlm.nih.gov/32948749/',
    },
    'DnSeg_v1': {
        'short_descrip': 'Basal Forebrain labeling',
        'inputs_descrip': 'T1w MRI',
        'procurl': 'https://github.com/DerekDoss/DnSeg',
    },
    'FALLYPRIDEQA_v2':{
        'short_descrip': 'Fallypride QA with Regional SUVR using whole cerebellum as reference.',
        'inputs_descrip': 'T1w MRI processed with FreeSurfer (FS7_v1), Fallypride PET',
        'procurl': 'https://github.com/bud42/FALLYPRIDEQA',
        'stats_subset': ['antcing_suvr', 'compositegm_suvr', 'cblmgm_suvr', 'cblmwm_suvr', 'cblmtot_suvr'],
    },
    'fmri_bct_v2': {
        'short_descrip': 'Brain Connectivity Toolbox measures.',
        'inputs_descrip': 'Resting MRI processed with fmri_roi_v2',
        'procurl': 'https://github.com/REMBRANDT-study/fmri_bct',
        'stats_subset': ['Shen268_thr0p3_degree', 'Schaefer400_thr0p3_degree'],
    },
    'fmri_msit_v2': {
        'short_descrip': 'fMRI MSIT task pre-processing and 1st-Level analysis.',
        'inputs_descrip': 'T1w MRI, MSIT fMRI, E-prime EDAT',
        'procurl': 'https://github.com/REMBRANDT-study/fmri_msit',
        'stats_subset': ['con_amyg_mean', 'inc_amyg_mean', 'med_pct_outliers', 'con_bnst_mean', 'inc_bnst_mean'],
    },
    'fmri_rest_v4': {
        'short_descrip': 'fMRI Resting State pre-processing.',
        'inputs_descrip': 'T1w MRI, Resting State fMRI',
        'procurl': 'https://github.com/REMBRANDT-study/fmri_rest',
    },
    'fmri_roi_v2': {
        'short_descrip': 'Regional measures of functional connectivity',
        'inputs_descrip': 'Resting State fMRI processed with fmri_rest_v2',
        'procurl': 'https://github.com/REMBRANDT-study/fmri_roi',
    },
    'FS7sclimbic_v0': {
        'short_descrip': 'FreeSurfer 7 ScLimbic - volumes of subcortical limbic regions including Basal Forebrain.',
        'inputs_descrip': 'T1w MRI processed with FreeSurfer (FS7_v1)',
        'procurl': 'https://surfer.nmr.mgh.harvard.edu/fswiki/ScLimbic',
        'stats_subset': ['Left-Basal-Forebrain', 'Right-Basal-Forebrain'],
    },
    'FEOBVQA_v2': {
        'short_descrip': 'Regional SUVR using Supra-ventricular White Matter as reference.',
        'inputs_descrip': 'T1w MRI processed with FreeSurfer (FS7_v1), FEOBV PET',
        'procurl': 'https://github.com/ccmvumc/FEOBVQA',
        'stats_subset': ['cblmwm_suvr', 'compositegm_suvr', 'cblmgm_suvr'],
    },
    'FS7_v1': {
        'short_descrip': 'FreeSurfer 7 recon-all - whole brain parcellation of cortical and sub-cortical.\n\
\n\
Outputs for cortical regions are volumes, thickness average, and surface area of left and right hemisphere for 34 parcels:\n\
bankssts, caudal anterior cingulate, caudal middle frontal, cuneus, entorhinal, frontal pole, \
fusiform, inferior parietal, inferior temporal, insula, isthmus cingulate, lateral occipital, \
lateral orbitofrontal, lingual, medial orbitofrontal, middle temporal, paracentral, \
parahippocampal, pars opercularis, pars orbitalis, pars triangularis, pericalcarine, \
postcentral, posterior cingulate, precentral, precuneus, rostral anterior cingulate, \
rostral middle frontal, superiorfrontal, superior parietal, superior temporal, \
supramarginal, temporal pole, transverse temporal.\n\
\n\
Outputs for subcortical structures are volumes for left and right hemispheres of 8 regions:\n\
accumbens area, amygdala, caudate, hippocampus, pallidum, putamen, thalamus, and \
ventraldc.\n\
\n\
The white matter hyperinsity volume (wmh) is whole brain.\n\
\n\
See examples.\n',
        'inputs_descrip': 'T1w MRI\n',
        'procurl': 'https://github.com/bud42/FS7',
        'stats_subset': [
            'amygdala_lh_volume',
            'amygdala_rh_volume',
            'accumbensarea_lh_volume',
            'accumbensarea_rh_volume',
            'caudate_lh_volume',
            'caudate_rh_volume',
            'hippocampus_lh_volume',
            'hippocampus_rh_volume',
            'pallidum_lh_volume',
            'pallidum_rh_volume',
            'putamen_lh_volume',
            'putamen_rh_volume',
            'thalamus_lh_volume',
            'thalamus_rh_volume',
            'entorhinal_lh_surfarea'
            'entorhinal_rh_surfarea',
            'entorhinal_lh_thickavg',
            'entorhinal_rh_thickavg',
            'entorhinal_lh_volume',
            'entorhinal_rh_volume',
            'frontalpole_lh_surfarea',
            'frontalpole_lh_thickavg',
            'frontalpole_lh_volume',
            'frontalpole_rh_surfarea',
            'frontalpole_rh_thickavg',
            'frontalpole_rh_volume',
            'insula_lh_surfarea',
            'insula_rh_surfarea',
            'insula_lh_thickavg',
            'insula_rh_thickavg',
            'insula_lh_volume',
            'insula_rh_volume',
            'superiorfrontal_lh_surfarea',
            'superiorfrontal_rh_surfarea',
            'superiorfrontal_lh_thickavg',
            'superiorfrontal_rh_thickavg',
            'superiorfrontal_lh_volume',
            'superiorfrontal_rh_volume',
        ],
    },
    'FS7HPCAMG_v1': {
        'short_descrip': 'FreeSurfer 7 hippocampus & amygdala sub-region volumes.',
        'inputs_descrip': 'T1w processed with FreeSurfer (FS7_v1)',
        'procurl': 'https://github.com/bud42/FS7HPCAMG_v1',
        'stats_subset': ['hpchead_lh', 'hpchead_rh', 'hpcbody_lh', 'hpcbody_rh', 'hpctail_lh', 'hpctail_rh'],
    },
    'LST_v1': {
        'short_descrip': 'Lesion Segmentation Toolbox - white matter lesion volumes.',
        'inputs_descrip': 'T1w MRI, FLAIR MRI',
        'procurl': 'https://github.com/ccmvumc/LST1',
    },
    'Multi_Atlas_v3': {
        'short_descrip': 'Multi Atlas Labeling with BrainColor atlas.',
        'inputs_descrip': 'T1w MRI',
        'procurl': 'https://pubmed.ncbi.nlm.nih.gov/27726243',
    },
    'SAMSEG_v1': {
        'short_descrip': 'Runs SAMSEG from FreeSurfer 7.2.',
        'inputs_descrip': 'T1w MRI processed with FreeSurfer (FS7_v1), FLAIR MRI',
        'procurl': 'https://surfer.nmr.mgh.harvard.edu/fswiki/Samseg',
    },
    'fmriqa_v4': {
        'inputs_descrip': 'T1w MRI processed with SLANT, fMRI',
        'short_descrip': 'Functional MRI QA',
        'procurl': 'https://github.com/baxpr/fmriqa',
        'stats_subset': ['dvars_mean', 'fd_mean'],
    },
    'fmri_emostroop_v2': {
        'short_descrip': 'fMRI EmoStroop Pre-processing and 1st-Level',
        'inputs_descrip': 'T1w MRI, fMRI, EDAT',
        'procurl': 'https://github.com/ccmvumc/fmri_emostroop:v2',
        'stats_subset': ['lhSFG2_incgtcon', 'rhSFG2_incgtcon', 'overall_rt_mean'],
    },
    'struct_preproc_v1': {
        'short_descrip': 'SPM Structural Pre-processing - segmentation/normalization',
        'inputs_descrip': 'T1w MRI, FLAIR MRI',
    },
    'FS7hypothal_v1': {
        'short_descrip': 'FreeSurfer Hypothalamic Subunit Segmentation',
        'inputs_descrip': 'T1w processed with FreeSurfer (FS7_v1)',
        'procurl': 'https://surfer.nmr.mgh.harvard.edu/fswiki/HypothalamicSubunits',
        'stats_subset': ['whole left', 'whole right'],
    }
}

STATLIB = {
    'BFC_v2': {
        'CH4_L_VOL': 'Basal Forebrain Left Hemisphere CH4 Volume',
        'CH4_R_VOL': 'Basal Forebrain Right Hemisphere CH4 Volume'
    },
    'BrainAgeGap_v2': {
        'bag_age_gap': 'Brain Age Gap',
        'bag_age_pred': 'Predicted Age',
    },
    'DnSeg_v1': {
        'nbm_lh': 'Nucleus Basalis of Meynert Left Hemisphere Volume',
        'nbm_rh': 'Nucleus Basalis of Meynert Right Hemisphere Volume',
    },
    'FALLYPRIDEQA_v2': {
        'accumbens_suvr': 'Accumbens regional mean SUVR normalized by Whole Cerebellum',
        'amygdala_suvr': 'Amygdala regional mean SUVR normalized by Whole Cerebellum',
        'antcing_suvr': 'Anterior Cingulate regional mean SUVR normalized by Whole Cerebellum',
        'antflobe_suvr': 'Anterior Frontal Lobe mean SUVR normalized by Whole Cerebellum',
        'caudate_suvr': 'Caudate regional mean SUVR normalized by Whole Cerebellum',
        'cblmgm_suvr': 'Cerebellum Gray Matter regional mean SUVR normalized by Whole Cerebellum',
        'cblmtot_suvr': 'Cerebellum Total regional mean SUVR normalized by Whole Cerebellum',
        'cblmwm_suvr': 'Cerebellum White Matter regional mean SUVR normalized by Whole Cerebellum',
        'compositegm_suvr': 'Composite Gray Matter regional mean SUVR normalized by Whole Cerebellum',
        'cortwm_suvr': 'Cortical White Matter regional mean SUVR normalized by Whole Cerebellum',
        'hippocampus_suvr': 'Hippocampus regional mean SUVR normalized by Whole Cerebellum',
        'latplobe_suvr': 'Lateral Parietal Lobe regional mean SUVR normalized by Whole Cerebellum',
        'lattlobe_suvr': 'Lateral Temporal Lobe regional mean SUVR normalized by Whole Cerebellum',
        'mofc_suvr': 'Medial Orbito-frontal Cortex regional mean SUVR normalized by Whole Cerebellum',
        'pallidum_suvr': 'Pallidum regional mean SUVR normalized by Whole Cerebellum',
        'postcing_suvr': 'Posterior Cingulate regional mean SUVR normalized by Whole Cerebellum',
        'putamen_suvr': 'Putamen regional mean SUVR normalized by Whole Cerebellum',
        'thalamus_suvr': 'Thalamus regional mean SUVR normalized by Whole Cerebellum',
        'ventraldc_suvr': 'Ventral Diencephalon regional mean SUVR normalized by Whole Cerebellum',
    },
    'FS7_v1': {
        'hippocampus_lh_volume': 'Hippocampus Volume Left Hemisphere',
        'hippocampus_rh_volume': 'Hippocampus Volume Right Hemisphere',
        'amygdala_lh_volume': 'Amygdala Volume Left Hemisphere',
        'amygdala_rh_volume': 'Amygdala Volume Right Hemisphere',
        'superiorfrontal_lh_surfarea': 'Superior Frontal Surface Area Left Hemisphere',
        'superiorfrontal_rh_surfarea': 'Superior Frontal Surface Area Right Hemisphere',
        'superiorfrontal_lh_thickavg': 'Superior Frontal Thickness Average Left Hemisphere',
        'superiorfrontal_rh_thickavg': 'Superior Frontal Thickness Average Right Hemisphere',
        'superiorfrontal_lh_volume': 'Superior Frontal Volume Left Hemisphere',
        'superiorfrontal_rh_volume': 'Superior Frontal Volume Right Hemisphere',
    },
    'FEOBVQA_v2': {
        'antcing_suvr': 'Anterior Cingulate SUVR normalized by Supra-ventricular White Matter',
        'antflobe_suvr': 'Anterior Frontal Lobe SUVR normalized by Supra-ventricular White Matter',
        'cblmgm_suvr': 'Cerebellar Gray Matter SUVR normalized by Supra-ventricular White Matter',
        'cblmwm_suvr': 'Cerebellar White Matter SUVR normalized by Supra-ventricular White Matter',
        'compositegm_suvr': 'Composite Gray Matter SUVR normalized by Supra-ventricular White Matter',
        'cblmgm_suvr': 'Cerebellar Gray Matter SUVR normalized by Supra-ventricular White Matter',
        'cortwm_eroded_suvr': 'Eroded Cortical White Matter SUVR normalized by Supra-ventricular White Matter',
        'latplobe_suvr': 'Lateral Parietal Lobe SUVR normalized by Supra-ventricular White Matter',
        'lattlobe_suvr': 'Lateral Temporal Lobe SUVR normalized by Supra-ventricular White Matter',
        'postcing_suvr': 'Posterior Cingulate SUVR normalized by Supra-ventricular White Matter',
    },
    'SAMSEG_v1': {
        'samseg_lesions': 'whole brain White Matter Lesion Volume in cubic millimeters',
        'samseg_sbtiv': 'segmentation-based (estimated) Total Intracranial Volume in cubic millimeters',
    },
    'FS7HPCAMG_v1': {
        'hpcbody_lh': 'Hippocampus Body Left Hemisphere Volume in cubic millimeters',
        'hpcbody_rh': 'Hippocampus Body Right Hemisphere Volume in cubic millimeters',
        'hpchead_lh': 'Hippocampus Head Left Hemisphere Volume in cubic millimeters',
        'hpchead_rh': 'Hippocampus Head Right Hemisphere Volume in cubic millimeters',
        'hpctail_lh': 'Hippocampus Tail Left Hemisphere Volume in cubic millimeters',
        'hpctail_rh': 'Hippocampus Tail Right Hemisphere Volume in cubic millimeters',
    },
    'FS7sclimbic_v0': {
        'Left-Basal-Forebrain': 'Basal Forebrain Left Hemisphere Volume in cubic millimeters',
        'Right-Basal-Forebrain': 'Basal Forebrain Right Hemisphere Volume in cubic millimeters',
    },
    'LST_v1': {
        'wml_volume': 'White Matter Lesion Volume in milliliters',
    },
    'fmriqa_v4': {
        'dvars_mean': 'Derivative of Variance of voxels (measure of signal consistency)',
        'fd_mean': 'Framewise Displacement (measure of motion between adjacent volumes)',
    },
    'struct_preproc_v1': {
        'Volume1': 'Gray Matter',
        'Volume2': 'White Matter',
        'Volume3': 'CSF',
    },
    'fmri_bct_v2': {
        'Schaefer400_thr0p1_deg': 'Degree'
    },
    'Multi_Atlas_v3': {
        'ticv_mm3': 'Total intracranial volume in cubic millimeters',
    },
    'FS7hypothal_v1': {
        'whole left': 'whole left Hypothalamus',
        'whole right': 'whole right Hypothalamus',
    },
}
