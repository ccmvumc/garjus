'''Subjects from REDCap.'''
import logging

import pandas as pd
import numpy as np

from ..utils_redcap import secondary, secondary_map, field2events


# Here we provide an export of a limited set of fields as determined here, later we'll
# make those fields dynamic with entries in REDcap. The list of proctypes is 
# determined by projects included. Each proctype is merged across projects.
# each project is a separate database so we load each project separately


logger = logging.getLogger('garjus.subjects.covariates')


# TODO: move this data to redcap entries
TYPE2PROJECTS = {
    'examiner': ['D3', 'COGD'],
    'plasma': ['D3'],
    'gaitrite': ['D3'],
    'toolbox': ['D3'],
    'fallypride': ['D3'],
    'msit_vitals': ['REMBRANDT'],
}


def _load_toolbox(rc):
    df = pd.DataFrame()
    def_field = rc.def_field
    fields = [
        def_field,
        'toolbox_picseqtest_uncstd',
        'toolbox_listsorttest_uncstd',
        'toolbox_patterntest_uncstd',
        'toolbox_picvocabtest_uncstd',
        'toolbox_oralrecogtest_uncstd',
        'toolbox_cogcrystalcomp_uncstd'
    ]

    # TODO: get this dynamically, these are D3 events
    events = [
        'baseline_arm_1',
        'baseline_arm_2',
        'baseline_arm_3',
        'baseline_arm_4'
    ]

    rec = rc.export_records(fields=fields)

    # Filter by selected events
    rec = [x for x in rec if x['redcap_event_name'] in events]

    rec = [x for x in rec if x['toolbox_picseqtest_uncstd']]

    # Format as dataframe of strings dropping any dupes
    df = pd.DataFrame(rec, columns=fields)
    df = df.astype(str)
    df = df.drop_duplicates(subset=[def_field], keep='first')

    # Set subject
    sec_field = secondary(rc)
    if sec_field:
        # Get the secondary field values to set the subject ID
        sec_map = secondary_map(rc)
        df['ID'] = df[def_field].map(sec_map)
    else:
        df['ID'] = df[def_field]

    df = df.rename(columns={
        'toolbox_picseqtest_uncstd': 'picseqtest_uncstd',
        'toolbox_listsorttest_uncstd': 'listsorttest_uncstd',
        'toolbox_patterntest_uncstd': 'patterntest_uncstd',
        'toolbox_picvocabtest_uncstd': 'picvocabtest_uncstd',
        'toolbox_oralrecogtest_uncstd': 'oralrecogtest_uncstd',
        'toolbox_cogcrystalcomp_uncstd': 'cogcrystalcomp_uncstd',
    })

    return df[[
        'ID',
        'picseqtest_uncstd',
        'listsorttest_uncstd',
        'patterntest_uncstd',
        'picvocabtest_uncstd',
        'oralrecogtest_uncstd',
        'cogcrystalcomp_uncstd'
        ]]


def _load_examiner(rc):
    df = pd.DataFrame()
    def_field = rc.def_field
    fields = [
        def_field,
        'executive_composite',
        'fluency_factor',
        'cog_control_factor',
        'working_memory_factor',
        'executive_se',
        'fluency_se',
        'cog_control_se',
        'working_memory_se',
    ]

    # TODO: get this dynamically, these are D3 events
    events = [
        'baseline_arm_1',
        'baseline_arm_2',
        'baseline_arm_3',
        'baseline_arm_4'
    ]

    rec = rc.export_records(fields=fields)

    # Filter by selected events
    rec = [x for x in rec if x['redcap_event_name'] in events]

    rec = [x for x in rec if x['executive_composite']]

    # Format as dataframe of strings dropping any dupes
    df = pd.DataFrame(rec, columns=fields)
    df = df.astype(str)
    df = df.drop_duplicates(subset=[def_field], keep='first')

    # Set subject
    sec_field = secondary(rc)
    if sec_field:
        # Get the secondary field values to set the subject ID
        sec_map = secondary_map(rc)
        df['ID'] = df[def_field].map(sec_map)
    else:
        df['ID'] = df[def_field]

    return df[[
        'ID',
        'executive_composite',
        'fluency_factor',
        'cog_control_factor',
        'working_memory_factor',
        'executive_se',
        'fluency_se',
        'cog_control_se',
        'working_memory_se',
        ]]


def _load_gaitrite(rc):
    df = pd.DataFrame()
    def_field = rc.def_field
    fields = [
        def_field,
        'gaitrite_comments',
        'gaitrite_velocity',
    ]

    # TODO: get this dynamically, these are D3 events
    events = [
        'baseline_arm_1',
        'baseline_arm_2',
        'baseline_arm_3',
        'baseline_arm_4'
    ]

    rec = rc.export_records(fields=fields)

    # Only gaitrite records
    rec = [x for x in rec if x['redcap_repeat_instrument'] == 'gaitrite']

    # Filter by selected events
    rec = [x for x in rec if x['redcap_event_name'] in events]

    # Only specific walks
    rec = [x for x in rec if \
        x['gaitrite_comments'].lower().startswith('standard') or \
        x['gaitrite_comments'].lower().startswith('first')
    ]

    # Format as dataframe of strings dropping any dupes
    df = pd.DataFrame(rec, columns=fields)
    df = df.astype(str)
    df = df.drop_duplicates(subset=[def_field], keep='first')

    # Rename
    df['SPEED'] = df['gaitrite_velocity']

    # Set subject
    sec_field = secondary(rc)
    if sec_field:
        # Get the secondary field values to set the subject ID
        sec_map = secondary_map(rc)
        df['ID'] = df[def_field].map(sec_map)
    else:
        df['ID'] = df[def_field]

    return df[['ID', 'SPEED']]


def _load_plasma(rc):
    df = pd.DataFrame()
    def_field = rc.def_field
    fields = [def_field, 'plasma_hscrp']
    events = field2events(rc, 'plasma_hscrp')

    rec = rc.export_records(fields=fields, events=events)

    # Filter by selected events
    # TODO: is this necessary?
    rec = [x for x in rec if x['redcap_event_name'] in events]

    rec = [x for x in rec if x['plasma_hscrp']]

    # Format as dataframe of strings dropping any dupes
    df = pd.DataFrame(rec, columns=fields)
    df = df.astype(str)
    df = df.drop_duplicates(subset=[def_field], keep='first')

    # Set subject
    sec_field = secondary(rc)
    if sec_field:
        # Get the secondary field values to set the subject ID
        sec_map = secondary_map(rc)
        df['ID'] = df[def_field].map(sec_map)
    else:
        df['ID'] = df[def_field]

    return df[['ID','plasma_hscrp']]


def _load_fallypride(rc):
    df = pd.DataFrame()
    def_field = rc.def_field
    fields = [def_field, 'fallypride_accumbens', 'fallypride_amygdala', 'fallypride_caudate', 'fallypride_pallidum', 'fallypride_putamen', 'fallypride_thalamus']
    events = field2events(rc, 'fallypride_accumbens')

    rec = rc.export_records(fields=fields, events=events)
    rec = [x for x in rec if x['redcap_event_name'] in events]
    rec = [x for x in rec if x['fallypride_accumbens']]

    # Format as dataframe of strings dropping any dupes
    df = pd.DataFrame(rec, columns=fields)
    df = df.astype(str)
    df = df.drop_duplicates(subset=[def_field], keep='first')

    # Set subject
    sec_field = secondary(rc)
    if sec_field:
        # Get the secondary field values to set the subject ID
        sec_map = secondary_map(rc)
        df['ID'] = df[def_field].map(sec_map)
    else:
        df['ID'] = df[def_field]

    return df[['ID', 'fallypride_accumbens', 'fallypride_amygdala', 'fallypride_caudate', 'fallypride_pallidum', 'fallypride_putamen', 'fallypride_thalamus']]


def _load_msit_vitals(rc):
    df = pd.DataFrame()
    def_field = rc.def_field
    fields = [
        def_field,
        't1_pulse2', 't1_pulse3',
        't1_sys2', 't1_sys3',
        't1_dia2', 't1_dia3',
        'msit_pulse1',  'msit_pulse2',  'msit_pulse3',  'msit_pulse4',
        'msit_dia1', 'msit_dia2', 'msit_dia3', 'msit_dia4',
        'msit_sys1', 'msit_sys2', 'msit_sys3', 'msit_sys4',
    ]
    events = field2events(rc, 'msit_pulse1')

    # TODO: get this dynamically, these are REMBRANDT events
    events = [
        'baselinemonth_0_arm_2',
        'baselinemonth_0_arm_3',
    ]

    rec = rc.export_records(fields=fields, events=events)
    rec = [x for x in rec if x['redcap_event_name'] in events]
    rec = [x for x in rec if x['msit_pulse1']]

    df = pd.DataFrame(rec, columns=fields + ['redcap_event_name'])

    # Set subject
    sec_field = secondary(rc)
    if sec_field:
        # Get the secondary field values to set the subject ID
        sec_map = secondary_map(rc)
        df['ID'] = df[def_field].map(sec_map)
    else:
        df['ID'] = df[def_field]

    df = df.replace('', np.nan).dropna(how='any', axis=0)

    df['rest_dbp'] = (df['t1_dia2'].astype('float') + df['t1_dia3'].astype('float')) / 2
    df['rest_pulse'] = (df['t1_pulse2'].astype('float') + df['t1_pulse3'].astype('float')) / 2
    df['rest_sbp'] = (df['t1_sys2'].astype('float') + df['t1_sys3'].astype('float')) / 2

    df['msit_dbp'] = (
        df['msit_dia1'].astype('float') + \
        df['msit_dia2'].astype('float') + \
        df['msit_dia3'].astype('float') + \
        df['msit_dia4'].astype('float')) / 4
    
    df['msit_pulse'] = (
        df['msit_pulse1'].astype('float') + \
        df['msit_pulse2'].astype('float') + \
        df['msit_pulse3'].astype('float') + \
        df['msit_pulse4'].astype('float')) / 4

    df['msit_sbp'] = (
        df['msit_sys1'].astype('float') + \
        df['msit_sys2'].astype('float') + \
        df['msit_sys3'].astype('float') + \
        df['msit_sys4'].astype('float')) / 4

    df['chg_pulse'] = df['msit_pulse'] - df['rest_pulse']
    df['chg_sbp'] = df['msit_sbp'] - df['rest_sbp']

    df = df.sort_values(['ID', 'redcap_event_name'])

    df.drop('record_id', axis=1)

    return df


def _proctype_projects(proctype, projects):
    proctype_projects = TYPE2PROJECTS.get(proctype, [])
    return list(set(projects) & set(proctype_projects))


def _export_toolbox(garjus, tmpdir, subjects_df):
    df = pd.DataFrame()

    projects = _proctype_projects('toolbox', subjects_df.PROJECT.unique())

    logger.debug(f'loading toolbox for projects:{projects}')

    for p in projects:
        # Load data for project
        logger.info(f'loading toolbox:{p}')
        _df = _load_toolbox(garjus.primary(p))

        # Filter by subjects
        logger.info(f'filtering by subject')
        subjects = subjects_df[subjects_df['PROJECT'] == p].ID.unique()
        _df = _df[_df.ID.isin(subjects)]

        # Append to whole
        _df['PROJECT'] = p
        df = pd.concat([df, _df], ignore_index=True)

    if len(df) > 0:
        # Save to csv
        _file = f'{tmpdir}/toolbox.csv'
        logger.info(f'saving to csv:{_file}')
        _cols = ['ID', 'PROJECT'] + [x for x in df.columns if x not in ['ID', 'PROJECT']]
        df= df.sort_values(['PROJECT', 'ID'])
        df.to_csv(_file, index=False, columns=_cols)
    else:
        logger.debug('no toolbox data to save')

    return df


def _export_examiner(garjus, tmpdir, subjects_df):
    df = pd.DataFrame()

    projects = _proctype_projects('examiner', subjects_df.PROJECT.unique())

    logger.debug(f'loading examiner for projects:{projects}')

    for p in projects:
        # Load examiner data for project
        logger.info(f'loading examiner:{p}')
        _df = _load_examiner(garjus.primary(p))

        # Filter by subjects
        logger.info(f'filtering by subject')
        subjects = subjects_df[subjects_df['PROJECT'] == p].ID.unique()
        _df = _df[_df.ID.isin(subjects)]

        # Append to whole
        _df['PROJECT'] = p
        df = pd.concat([df, _df], ignore_index=True)

    if len(df) > 0:
        # Save to csv
        _file = f'{tmpdir}/examiner.csv'
        logger.info(f'saving to csv:{_file}')
        _cols = ['ID', 'PROJECT'] + [x for x in df.columns if x not in ['ID', 'PROJECT']]
        df= df.sort_values(['PROJECT', 'ID'])
        df.to_csv(_file, index=False, columns=_cols)
    else:
        logger.debug('no examiner data to save')

    return df


def _export_gaitrite(garjus, tmpdir, subjects_df):
    df = pd.DataFrame()

    projects = _proctype_projects('gaitrite', subjects_df.PROJECT.unique())

    logger.debug(f'loading gaitrite for projects:{projects}')

    for p in projects:
        # Load gaitrite data for project
        logger.info(f'loading gaitrite:{p}')
        _df = _load_gaitrite(garjus.primary(p))

        # Filter by subjects
        subjects = subjects_df[subjects_df['PROJECT'] == p].ID.unique()
        _df = _df[_df.ID.isin(subjects)]

        # Append to whole
        _df['PROJECT'] = p
        df = pd.concat([df, _df], ignore_index=True)

    if len(df) > 0:
        # Save to csv
        _file = f'{tmpdir}/gaitrite.csv'
        logger.info(f'saving to csv:{_file}')
        _cols = ['ID', 'PROJECT'] + [x for x in df.columns if x not in ['ID', 'PROJECT']]
        df = df.sort_values(['PROJECT', 'ID'])
        df.to_csv(_file, index=False, columns=_cols)
    else:
        logger.debug('no gaitrite data to save')

    return df


def _export_plasma(garjus, tmpdir, subjects_df):
    df = pd.DataFrame()

    projects = _proctype_projects('plasma', subjects_df.PROJECT.unique())

    logger.debug(f'loading plasma for projects:{projects}')

    for p in projects:
        # Load plasma data for project
        logger.info(f'loading plasma:{p}')
        _df = _load_plasma(garjus.primary(p))

        # Filter by subjects
        subjects = subjects_df[subjects_df['PROJECT'] == p].ID.unique()
        _df = _df[_df.ID.isin(subjects)]

        # Append to whole
        _df['PROJECT'] = p
        df = pd.concat([df, _df], ignore_index=True)

    if len(df) > 0:
        # Save to csv
        _file = f'{tmpdir}/plasma.csv'
        logger.info(f'saving to csv:{_file}')
        _cols = ['ID', 'PROJECT'] + [x for x in df.columns if x not in ['ID', 'PROJECT']]
        df = df.sort_values(['PROJECT', 'ID'])
        df.to_csv(_file, index=False, columns=_cols)
    else:
        logger.debug('no plasma data to save')

    return df


def _export_fallypride(garjus, tmpdir, subjects_df):
    df = pd.DataFrame()
    proctype = 'fallypride'
    projects = _proctype_projects(proctype, subjects_df.PROJECT.unique())

    logger.debug(f'loading {proctype} for projects:{projects}')

    for p in projects:
        # Load data for project
        logger.info(f'loading {proctype}:{p}')
        _df = _load_fallypride(garjus.primary(p))

        # Filter by subjects
        subjects = subjects_df[subjects_df['PROJECT'] == p].ID.unique()
        _df = _df[_df.ID.isin(subjects)]

        # Append to whole
        _df['PROJECT'] = p
        df = pd.concat([df, _df], ignore_index=True)

    if len(df) > 0:
        # Save to csv
        _file = f'{tmpdir}/{proctype}.csv'
        logger.info(f'saving to csv:{_file}')
        _cols = ['ID', 'PROJECT'] + [x for x in df.columns if x not in ['ID', 'PROJECT']]
        df = df.sort_values(['PROJECT', 'ID'])
        df.to_csv(_file, index=False, columns=_cols)
    else:
        logger.debug(f'no {proctype} data to save')

    return df


def _export_msit_vitals(garjus, tmpdir, subjects_df):
    df = pd.DataFrame()
    proctype = 'msit_vitals'
    projects = _proctype_projects(proctype, subjects_df.PROJECT.unique())

    logger.debug(f'loading {proctype} for projects:{projects}')

    for p in projects:
        # Load data for project
        logger.info(f'loading {proctype}:{p}')
        _df = _load_msit_vitals(garjus.primary(p))

        # Filter by subjects
        subjects = subjects_df[subjects_df['PROJECT'] == p].ID.unique()
        _df = _df[_df.ID.isin(subjects)]

        # Append to whole
        _df['PROJECT'] = p
        df = pd.concat([df, _df], ignore_index=True)

    if len(df) > 0:
        # Save to csv
        _file = f'{tmpdir}/{proctype}.csv'
        logger.info(f'saving to csv:{_file}')
        _cols = ['ID', 'PROJECT'] + [x for x in df.columns if x not in ['ID', 'PROJECT']]
        df = df.sort_values(['PROJECT', 'ID'])
        df.to_csv(_file, index=False, columns=_cols)
    else:
        logger.debug(f'no {proctype} data to save')

    return df



def export_clinical():
    df = pd.DataFrame()

    # Load D3 clinical data

    # MADRS baseline
    # ma_tot
    # madrs_date

    # ACS baseline
    # acs_focus
    # acs_shift
    # acs_sum
    # acs_date

    # QIDS
    # qidsc_total
    # qids_date

    # AES
    # aes_total
    # aes_date
    # aes_totalc
    # aes_totalb
    # aes_totale
    # aes_totalo

    return df


def export(garjus, tmpdir, subjects_df):
    data = {}

    # Save csv for each type filtered by subject list
    _data = _export_examiner(garjus, tmpdir, subjects_df)
    if not _data.empty:
        _data['SITE'] = 'VUMC'
        _data.loc[_data.ID.str.startswith('P'), 'SITE'] = 'UPMC'
        data['examiner'] = _data

    _data = _export_gaitrite(garjus, tmpdir, subjects_df)
    if not _data.empty:
        _data['SITE'] = 'VUMC'
        _data.loc[_data.ID.str.startswith('P'), 'SITE'] = 'UPMC'
        data['gaitrite'] = _data 

    _data = _export_plasma(garjus, tmpdir, subjects_df)
    if not _data.empty:
        _data['SITE'] = 'VUMC'
        _data.loc[_data.ID.str.startswith('P'), 'SITE'] = 'UPMC'
        data['plasma'] = _data

    _data = _export_toolbox(garjus, tmpdir, subjects_df)
    if not _data.empty:
        _data['SITE'] = 'VUMC'
        _data.loc[_data.ID.str.startswith('P'), 'SITE'] = 'UPMC'
        data['toolbox'] = _data

    _data = _export_fallypride(garjus, tmpdir, subjects_df)
    if not _data.empty:
        _data['SITE'] = 'VUMC'
        data['fallypride'] = _data

    _data = _export_msit_vitals(garjus, tmpdir, subjects_df)
    if not _data.empty:
        _data['SITE'] = 'VUMC'
        _data.loc[_data.ID.str.startswith('2'), 'SITE'] = 'UPMC'
        _data.loc[_data.ID.str.startswith('3'), 'SITE'] = 'UIC'
        data['msit_vitals'] = _data

    return data
