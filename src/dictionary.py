ACTIVITY_RENAME = {
    'redcap_repeat_instance': 'ID',
    'activity_description': 'DESCRIPTION',
    'activity_datetime': 'DATETIME',
    'activity_event': 'EVENT',
    'activity_field': 'FIELD',
    'activity_result': 'RESULT',
    'activity_scan': 'SCAN',
    'activity_subject': 'SUBJECT',
    'activity_session': 'SESSION',
    'activity_type': 'CATEGORY',
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
    'processor_file': 'FILE',
    'processor_custom': 'CUSTOM',
    'processor_filter': 'FILTER',
    'processor_args': 'ARGS',
}

TASKS_RENAME = {
    'task_assessor': 'ASSESSOR',
    'task_status': 'STATUS',
    'task_cmds': 'CMDS',
    'task_memreq': 'MEMREQ',
    'task_walltime': 'WALLTIME',
    'task_procdate': 'PROCDATE',
    'task_timeused': 'TIMEUSED',
    'task_memused': 'MEMUSED',
}

COLUMNS = {
    'activity': ['PROJECT', 'SUBJECT', 'SESSION', 'SCAN', 'ID', 'DESCRIPTION',
                 'DATETIME', 'EVENT', 'FIELD', 'CATEGORY', 'RESULT', 'STATUS'],
    'assessors': ['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'DATE', 'SITE',
                  'ASSR', 'PROCSTATUS', 'PROCTYPE', 'JOBDATE', 'QCSTATUS',
                  'QCDATE', 'QCBY', 'XSITYPE', 'INPUTS', 'MODALITY', 'full_path'],
    'issues': ['PROJECT', 'SUBJECT', 'SESSION', 'SCAN ', 'ID', 'DESCRIPTION',
               'DATETIME', 'EVENT', 'FIELD', 'CATEGORY', 'STATUS'],
    'scans': ['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'TRACER', 'DATE',
              'SITE', 'SCANID', 'SCANTYPE', 'QUALITY', 'RESOURCES', 'MODALITY', 'XSITYPE', 'full_path'],
    'processing': ['PROJECT', 'TYPE', 'FILTER', 'FILE', 'CUSTOM', 'ARGS'],
    'subjects': ['PROJECT', 'SUBJECT', 'AGE', 'SEX', 'RACE'],
    'tasks': ['NAME', 'MEMREQ', 'WALLTIME', 'TIMEUSED', 'MEMUSED', 'ASSESSOR', 'PROCDATE', 'CMDS', 'IMAGEDIR', 'JOBTEMPLATE', 'YAMLFILE'],
}

# TODO: load this information from processor yamls or put it in REDCap?
# Processor main record with repeating stats
# Processor fields: ID(name_vX), name, short_descrip, inputs_descrip, procurl
# Stat fields: ID(auto), name, descrip

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
    'FEOBVQA_v2': {
        'short_descrip': 'Regional Amyloid SUVR using cerebellum as reference.',
        'inputs_descrip': 'T1w MRI processed with FreeSurfer (FS7_v1), FEOBV PET',
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
    'FS7_v1': {
        'fs7_etiv': 'Estimated Total Intracranial Volume',
        'fs7_hpc_lh': 'Hippocampus Left Hemisphere Volume',
        'fs7_hpc_rh': 'Hippocampus Right Hemisphere Volume',
        'fs7_latvent_lh': 'Lateral Ventricle Left Hemisphere Volume',
        'fs7_latvent_rh': 'Lateral Ventricle Right Hemisphere Volume',
        'fs7_stnv': 'Supra-tentorial not ventricles Volume',
        'fs7_supflobe_lh': 'Superior Frontal Lobe Left Hemisphere Thickness',
        'fs7_supflobe_rh': 'Superior Frontal Lobe Right Hemisphere Thickness',
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
    },
    'LST_v1': {
        'wml_volume': 'White Matter Lesion Volume',
    },
    'fmriqa_v4': {
        'dvars_mean': 'DVARS, framewise signal',
        'fd_mean': 'Framewise Displacement',
    },
    'struct_preproc_v1': {
        'Volume1': 'Gray Matter',
        'Volume2': 'White Matter',
        'Volume3': 'CSF',
    },
    'fmri_bct_v1': {
        'Schaefer400_thr0p1_deg': 'Degree'
    }
}
# EDATQA
# FS7sclimbic_v0
# fmri_emostroop_v2
# fmri_msit_v2 statlib
# fmri_nback_v2
# BFC_v2
# BrainAgeGap_v2
