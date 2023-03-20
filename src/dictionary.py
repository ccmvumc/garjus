COLUMNS = {
    'activity': ['PROJECT', 'SUBJECT', 'SESSION', 'SCAN', 'ID', 'DESCRIPTION',
                 'DATETIME', 'EVENT', 'FIELD', 'CATEGORY', 'RESULT', 'STATUS'],
    'assessors': ['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'DATE', 'SITE',
                  'ASSR', 'PROCSTATUS', 'PROCTYPE', 'JOBDATE', 'QCSTATUS',
                  'QCDATE', 'QCBY', 'XSITYPE', 'INPUTS', 'MODALITY'],
    'issues': ['PROJECT', 'SUBJECT', 'SESSION', 'SCAN ', 'ID', 'DESCRIPTION',
               'DATETIME', 'EVENT', 'FIELD', 'CATEGORY', 'STATUS'],
    'scans': ['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'TRACER', 'DATE',
              'SITE', 'SCANID', 'SCANTYPE', 'QUALITY', 'RESOURCES', 'MODALITY']
}


# TODO: load this information from processor yamls
PROCLIB = {
    'AMYVIDQA_v1': {
        'short_descrip': 'Regional Amyloid SUVR using cerebellum as reference.',
        'inputs_descrip': 'T1w MRI processed with FreeSurfer (FS7_v1), Amyloid PET',
        'procurl': 'https://github.com/ccmvumc/AMYVIDQA',
    },
    'BFC_v2': {
        'short_descrip': 'Basal Forebrain Volumes.',
        'inputs_descrip': 'T1w MRI',
        'procurl': 'https://github.com/ccmvumc/BFC',
    },
    'BrainAgeGap_v2': {
        'short_descrip': 'Predicted age of brain.',
        'inputs_descrip': 'T1w MRI parcellated with BrainColor atlas',
        'procurl': 'https://pubmed.ncbi.nlm.nih.gov/32948749/',
    },
    'fmri_bct_v1': {
        'short_descrip': 'Brain Connectivity Toolbox measures.',
        'inputs_descrip': 'Resting MRI processed with fmri_roi_v2',
        'procurl': 'https://github.com/REMBRANDT-study/fmri_bct',
    },
    'fmri_msit_v2': {
        'short_descrip': 'fMRI MSIT task pre-processing and 1st-Level analysis.',
        'inputs_descrip': 'T1w MRI, MSIT fMRI, E-prime EDAT',
        'procurl': 'https://github.com/REMBRANDT-study/fmri_msit',
    },
    'fmri_rest_v2': {
        'short_descrip': 'fMRI Resting State pre-processing.',
        'inputs_descrip': 'T1w MRI, Resting State fMRI',
        'procurl': 'https://github.com/REMBRANDT-study/fmri_rest',
    },
    'fmri_roi_v2': {
        'short_descrip': 'Regional measures of functional connectivity',
        'inputs_descrip': 'Resting State fMRI processed with fmri_rest_v2',
        'procurl': 'https://github.com/REMBRANDT-study/fmri_roi',
    },
    'FS7_sclimbic_v0': {
        'short_descrip': 'FreeSurfer 7 ScLimbic - volumes of subcortical limbic regions including Basal Forebrain.',
        'inputs_descrip': 'T1w MRI processed with FreeSurfer (FS7_v1)',
        'procurl': 'https://surfer.nmr.mgh.harvard.edu/fswiki/ScLimbic',
    },
    'FEOBVQA_v1': {
        'short_descrip': 'Regional Amyloid SUVR using cerebellum as reference.',
        'inputs_descrip': 'T1w MRI processed with FreeSurfer (FS7_v1), Amyloid PET',
        'procurl': 'https://github.com/ccmvumc/AMYVIDQA',
    },
    'FS7_v1': {
        'short_descrip': 'FreeSurfer 7 recon-all - whole brain parcellation, surfaces, cortical thickness.',
        'inputs_descrip': 'T1w MRI',
        'procurl': 'https://github.com/bud42/FS7',
    },
    'FS7HPCAMG_v1': {
        'short_descrip': 'FreeSurfer 7 hippocampus & amygdala sub-region volumes.',
        'inputs_descrip': 'T1w processed with FreeSurfer (FS7_v1)',
        'procurl': 'https://github.com/bud42/FS7HPCAMG_v1',
    },
    'LST_v1': {
        'short_descrip': 'Lesion Segmentation Toolbox - white matter lesion volumes.',
        'inputs_descrip': 'T1w MRI, FLAIR MRI',
        'procurl': 'https://github.com/ccmvumc/LST1',
    },
    'SAMSEG_v1': {
        'short_descrip': 'Runs SAMSEG from FreeSurfer 7.2 to get White Matter Lesion Volume.',
        'inputs_descrip': 'T1w MRI processed with FreeSurfer (FS7_v1), FLAIR MRI',
        'procurl': 'https://surfer.nmr.mgh.harvard.edu/fswiki/Samseg',
    },
}


STATLIB = {
    'FEOBVQA_v1': {
        'antcing_suvr': 'Anterior Cingulate Standardized Uptake Value Ratio',
        'antflobe_suvr': 'Anterior Frontal Lobe Standardized Uptake Value Ratio',
        'cblmgm_suvr': 'Cerebellar Gray Matter Standardized Uptake Value Ratio',
        'cblmwm_suvr': 'Cerebellar White Matter Standardized Uptake Value Ratio',
        'compositegm_suvr': 'Composite Gray Matter Standardized Uptake Value Ratio',
        'cblmgm_suvr': 'Cerebellar Gray Matter Standardized Uptake Value Ratio',
        'cortwm_eroded_suvr': 'Eroded Cortical White Matter Standardized Uptake Value Ratio',
        'latplobe_suvr': 'Lateral Parietal Lobe Standardized Uptake Value Ratio',
        'lattlobe_suvr': 'Lateral Temporal Lobe Standardized Uptake Value Ratio',
        'postcing_suvr': 'Posterior Cingulate Standardized Uptake Value Ratio',
    },
    'SAMSEG_v1': {
        'samseg_lesions':  'whole brain White Matter Lesion Volume in cubic millimeters',
        'samseg_sbtiv': 'segmentation-based (estimated) Total Intracranial Volume in cubic millimeters',
    },
    'FS7HPCAMG_v1': {
        'amgwhole_lh': 'Amygdala Whole Left Hemisphere Volume in cubic millimeters',
        'amgwhole_rh': 'Amygdala Whole Right Hemisphere Volume in cubic millimeters',
        'hpcbody_lh': 'Hippocampus Body Left Hemisphere Volume in cubic millimeters',
        'hpcbody_rh': 'Hippocampus Body Right Hemisphere Volume in cubic millimeters',
        'hpchead_lh': 'Hippocampus Head Left Hemisphere Volume in cubic millimeters',
        'hpchead_rh': 'Hippocampus Head Right Hemisphere Volume in cubic millimeters',
        'hpctail_lh': 'Hippocampus Tail Left Hemisphere Volume in cubic millimeters',
        'hpctail_rh': 'Hippocampus Tail Right Hemisphere Volume in cubic millimeters',
        'hpcwhole_lh': 'Hippocampus Whole Left Hemisphere Volume in cubic millimeters',
        'hpcwhole_rh': 'Hippocampus Whole Right Hemisphere Volume in cubic millimeters',
    }
}