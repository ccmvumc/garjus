'''Subjects from REDCap.'''
import logging

import pandas as pd
import numpy as np

logger = logging.getLogger('garjus.subjects')


def load_gait_data(rc):
    def_field = rc.def_field
    fields = [
        def_field,
        'gaitrite_comments',
        'gaitrite_velocity',
    ]

    events = [
        'baseline_arm_1',
        'baseline_arm_2',
        'baseline_arm_3',
        'baseline_arm_4'
    ]

    data = []

    rec = rc.export_records(fields=fields)

    # Only gaitrite records
    rec = [x for x in rec if x['redcap_repeat_instrument'] == 'gaitrite']

    # Filter by selected events
    rec = [x for x in rec if x['redcap_event_name'] in events]

    rec = [x for x in rec if \
        x['gaitrite_comments'].lower().startswith('standard') or \
        x['gaitrite_comments'].lower().startswith('first')
    ]

    dfg = pd.DataFrame(rec, columns=fields)

    dfg = dfg.astype(str)
    
    dfg = dfg.drop_duplicates(subset=[def_field], keep='first')

    dfg['SPEED'] = dfg['gaitrite_velocity']

    return dfg[[def_field, 'SPEED']]


def load_R21Perfusion(garjus):
    R21_SCREEN_FIELDS = [
        'participant_id',
        'study_ident',
        'madrs_same',
        'ma_tot',
        'crit_meets',
        'critcon_meets']

    R21_BASE_FIELDS = [
        'participant_id',
        'madrs_same',
        'ma_tot',
        'age',
        'dob',
        'sex_xcount']

    rc = garjus.primary('R21Perfusion')

    screenR = rc.export_records(
        raw_or_label='label',
        format_type='df',
        fields=R21_SCREEN_FIELDS,
        events=['screen_arm_1'])
    baseR = rc.export_records(
        raw_or_label='label',
        format_type='df',
        fields=R21_BASE_FIELDS,
        events=['baseline_arm_1'])

    # Change index
    screenR.reset_index(inplace=True)
    screenR.set_index('participant_id', inplace=True)
    screenR.drop(['redcap_event_name'], inplace=True, axis=1)

    # Change index
    baseR.reset_index(inplace=True)
    baseR.set_index('participant_id', inplace=True)
    baseR.drop(['redcap_event_name'], inplace=True, axis=1)

    # Set the group
    screenR = screenR.apply(get_group_R21Perfusion, axis=1)

    # Prep to later merge MADRS
    screenR['ma_tot_screen'] = screenR['ma_tot']
    screenR['madrs_same_screen'] = screenR['madrs_same']
    screenR.drop(['ma_tot','madrs_same'], inplace=True, axis=1)
    baseR['ma_tot_base'] = baseR['ma_tot']
    baseR['madrs_same_base'] = baseR['madrs_same']
    baseR.drop(['ma_tot','madrs_same'], inplace=True, axis=1)

    # Merge Baseline and Screen
    dataR = pd.merge(
        screenR,
        baseR,
        how='outer',
        left_index=True,
        right_index=True,
        sort=True)

    # Merge MADRS
    dataR = dataR.apply(get_base_madrs, axis=1)
    dataR.drop(
        ['ma_tot_base', 'madrs_same_base', 'ma_tot_screen', 'madrs_same_screen'],
        inplace=True, axis=1)

    # Change index to ID
    dataR['study_ident'] = dataR['study_ident'].astype(int).astype(str)
    dataR.rename(columns={'study_ident': 'ID'}, inplace=True)
    dataR.reset_index(inplace=True)
    dataR.set_index('ID', inplace=True)
    dataR.drop(['participant_id'], inplace=True, axis=1)

    # Set common fields
    dataR['PROJECT'] = 'R21Perfusion'
    dataR['AGE'] = dataR['age']
    dataR = dataR[~dataR.dob.isna()]
    dataR['DOB'] = pd.to_datetime(dataR['dob'])
    dataR['SEX'] = dataR['sex_xcount'].map({'Male': 'M', 'Female': 'F'})

    return dataR


def get_group_R21Perfusion(row):
    if row['crit_meets'] == 'Yes' and row['critcon_meets'] == 'Yes':
        print('ERROR:cannot be both DEPRESSED and CONTROL')
    elif row['crit_meets'] == 'Yes':
        row['GROUP'] = 'Depress'
    elif row['critcon_meets'] == 'Yes':
        row['GROUP'] = 'Control'

    return row


def get_base_madrs(row):    
    base_madrs = row['ma_tot_base']
    screen_madrs = row['ma_tot_screen']
    screen_same = row['madrs_same_screen']

    if np.isfinite(base_madrs):
        row['ma_tot'] = base_madrs
    elif ~np.isfinite(base_madrs) and np.isfinite(screen_madrs) and screen_same:
        row['ma_tot'] = screen_madrs
    elif ~np.isfinite(base_madrs) and np.isfinite(screen_madrs):
        print('using screen madrs:'+str(row['study_ident']))
        row['ma_tot'] = screen_madrs
    else:
        pass

    return row


def load_CAARE(garjus):
    C1_SCREEN_FIELDS = [
        'participant_id',
        'study_ident',
        'madrs_same',
        'ma_tot',
        'mmse_total',
        'age',
        'dob',
        'sex_xcount']

    C1_BASE_FIELDS = [
        'participant_id',
        'madrs_same',
        'ma_tot']

    C2_SCREEN_FIELDS = [
        'participant_id',
        'study_ident',
        'madrs_same',
        'ma_tot',
        'mmse_total',
        'age',
        'dob',
        'sex_xcount']

    C2_BASE_FIELDS = [
        'participant_id',
        'madrs_same',
        'ma_tot']

    rc = garjus.primary('TAYLOR_CAARE')

    screenC1 = rc.export_records(
        raw_or_label='label', format_type='df', fields=C1_SCREEN_FIELDS, events=['screen_arm_1'])
    baseC1 = rc.export_records(
        raw_or_label='label', format_type='df', fields=C1_BASE_FIELDS, events=['baseline_arm_1'])
    screenC2 = rc.export_records(
        raw_or_label='label', format_type='df', fields=C2_SCREEN_FIELDS, events=['screen_arm_2'])
    baseC2 = rc.export_records(
        raw_or_label='label', format_type='df', fields=C2_BASE_FIELDS, events=['baseline_arm_2'])

    # Change index
    screenC1.reset_index(inplace=True)
    screenC1.set_index('participant_id', inplace=True)
    screenC1.drop(['redcap_event_name'], inplace=True, axis=1)

    # Change index
    baseC1.reset_index(inplace=True)
    baseC1.set_index('participant_id', inplace=True)
    baseC1.drop(['redcap_event_name'], inplace=True, axis=1)

    # Deal with MADRS
    screenC1['ma_tot_screen'] = screenC1['ma_tot']
    screenC1['madrs_same_screen'] = screenC1['madrs_same']
    screenC1.drop(['ma_tot','madrs_same'], inplace=True, axis=1)
    baseC1['ma_tot_base'] = baseC1['ma_tot']
    baseC1['madrs_same_base'] = baseC1['madrs_same']
    baseC1.drop(['ma_tot','madrs_same'], inplace=True, axis=1)

    # Now merge screen and base
    dataC1 = pd.merge(screenC1, baseC1, how='outer', left_index=True, right_index=True, sort=True)
    dataC1 = dataC1[np.isfinite(dataC1['study_ident'])]
    dataC1['study_ident'] = dataC1['study_ident'].astype(int).astype(str)
    dataC1 = dataC1.apply(get_base_madrs, axis=1)

    # Change index
    screenC2.reset_index(inplace=True)
    screenC2.set_index('participant_id', inplace=True)
    screenC2.drop(['redcap_event_name'], inplace=True, axis=1)

    # Change index
    baseC2.reset_index(inplace=True)
    baseC2.set_index('participant_id', inplace=True)
    baseC2.drop(['redcap_event_name'], inplace=True, axis=1)

    # Deal with MADRS
    screenC2['ma_tot_screen'] = screenC2['ma_tot']
    screenC2['madrs_same_screen'] = screenC2['madrs_same']
    screenC2.drop(['ma_tot','madrs_same'], inplace=True, axis=1)
    baseC2['ma_tot_base'] = baseC2['ma_tot']
    baseC2['madrs_same_base'] = baseC2['madrs_same']
    baseC2.drop(['ma_tot','madrs_same'], inplace=True, axis=1)

    # Now merge screen and base
    dataC2 = pd.merge(
        screenC2, baseC2,
        how='outer', left_index=True, right_index=True, sort=True)
    dataC2['study_ident'] = dataC2['study_ident'].astype(int).astype(str)
    dataC2 = dataC2.apply(get_base_madrs, axis=1)

    # Concat arms
    dataC1['GROUP'] = 'Depress'
    dataC2['GROUP'] = 'Control'
    dataC = pd.concat([dataC1, dataC2], sort=True)

    # Change index to ID
    dataC.rename(columns={'study_ident': 'ID'}, inplace=True)
    dataC.reset_index(inplace=True)
    dataC.set_index('ID', inplace=True)
    dataC.drop(['participant_id'], inplace=True, axis=1)

    # Drop unused columns
    dataC.drop(
        ['ma_tot_base', 'madrs_same_base', 'ma_tot_screen', 'madrs_same_screen'],
        inplace=True, axis=1)

    dataC['PROJECT'] = 'TAYLOR_CAARE'
    dataC['AGE'] = dataC['age']
    dataC['DOB'] = pd.to_datetime(dataC['dob'])
    dataC['SEX'] = dataC['sex_xcount'].map({'Male': 'M', 'Female': 'F'})

    dataC = dataC[~dataC.DOB.isna()]

    return dataC


def load_MDDHx():
    controls = [
        '2503', '2505', '2510', '2511', '2513', '2518', '2520', '2522', '2526',
        '2527', '2529', '2531', '2532', '2535', '2536', '2537', '2538', '2539',
        '2540', '2541', '2542', '2543', '2544', '2547', '2551', '2552', '2553',
        '2554', '2556', '2558', '2561', '2563', '2569', '2574', '2575', '2577',
        '2578', '2579', '2580', '2581', '2582', '2585', '2586', '2588', '2592'
        ]

    age = [
        64, 62, 63, 69, 68, 66, 74, 54, 60, 68, 70, 72, 57, 54, 63, 58, 53, 53,
        61, 67, 55, 54, 56, 57, 66, 65, 58, 58, 53, 71, 56, 54, 59, 55, 51, 67,
        52, 70, 60, 59, 56, 66, 67, 70, 58]

    df = pd.DataFrame({'ID': controls})
    df['GROUP'] = 'Control'
    df['PROJECT'] = 'NewhouseMDDHx'
    df['AGE'] = age
    df['SEX'] = 'F'

    return df


def load_COGD(garjus):
    dob_field = 'backg_dob'
    sex_field = 'backg_sex'
    date_field = 'mri_date'
    fields = [dob_field, sex_field, date_field]

    project_redcap = garjus.primary('COGD')

    rec = project_redcap.export_records(fields=fields, raw_or_label='label')

    try:
        rec = [x for x in rec if x[dob_field]]
    except KeyError as err:
        logger.debug(f'cannot access dob:{dob_field}:{err}')

    df = pd.DataFrame(rec, columns=fields + ['redcap_event_name'])

    # all are depressed
    df['GROUP'] = 'Depress'

    return df


def load_subjects(garjus, project, include_dob=False):
    project_redcap = garjus.primary(project)

    if project == 'NewhouseMDDHx':
        return load_MDDHx()

    if not project_redcap:
        logger.debug(f'project redcap not found:{project}')
        return pd.DataFrame([], columns=['ID', 'PROJECT', 'GROUP'])

    if project == 'R21Perfusion':
        return load_R21Perfusion(garjus)        
    elif project == 'TAYLOR_CAARE':
        return load_CAARE(garjus)

    def_field = project_redcap.def_field
    sec_field = project_redcap.export_project_info()['secondary_unique_field']
    guid_field = None
    sex_field = None
    dob_field = None
    date_field = None
    field_names = project_redcap.field_names

    if 'guid' in field_names:
        guid_field = 'guid'
    elif 'guid_5b4861' in field_names:
        guid_field = 'guid_5b4861'

    if 'dob' in field_names:
        dob_field = 'dob'
    elif 'dob_sub' in field_names:
        dob_field = 'dob_sub'
    elif 'backg_dob' in field_names:
        dob_field = 'backg_dob'

    if 'sex_xcount' in field_names:
        sex_field = 'sex_xcount'
    elif 'dems_sex' in field_names:
        sex_field = 'dems_sex'
    elif 'sex_demo' in field_names:
        sex_field = 'sex_demo'
    elif 'backg_sex' in field_names:
        sex_field = 'backg_sex'

    if 'mri_date' in field_names:
        date_field = 'mri_date'

    # Load subject records from redcap
    fields = [def_field]

    if sec_field:
        fields.append(sec_field)

    if guid_field:
        fields.append(guid_field)

    if dob_field:
        fields.append(dob_field)

    if sex_field:
        fields.append(sex_field)

    rec = project_redcap.export_records(fields=fields, raw_or_label='label')

    # Ignore records without secondary ID
    if sec_field:
        rec = [x for x in rec if x[sec_field]]

    # Must have dob to calc age
    if dob_field:
        try:
            rec = [x for x in rec if x[dob_field]]
        except KeyError as err:
            logger.debug(f'cannot access dob:{dob_field}:{err}')

    # Make data frame
    if project_redcap.is_longitudinal:
        df = pd.DataFrame(rec, columns=fields + ['redcap_event_name'])
    else:
        df = pd.DataFrame(rec, columns=fields)

    # Set the project
    df['PROJECT'] = project

    # Determine group
    df['GROUP'] = 'UNKNOWN'

    if project in ['DepMIND2', 'DepMIND3', 'COGD']:
        # All are depressed
        df['GROUP'] = 'Depress'
    elif project == 'D3':
        # Load gait velocity
        dfg = load_gait_data(project_redcap)

        # Merge in gait data
        df = pd.merge(df, dfg, how='left', on=def_field)

        # Use arm/events names to determine group
        df['GROUP'] = df['redcap_event_name'].map({
            'Screening (Arm 2: VUMC Never Depressed)': 'Control',
            'Screening (Arm 1: VUMC Currently Depressed)': 'Depress',
            'Screening (Arm 4: UPMC Never Depressed)': 'Control',
            'Screening (Arm 3: UPMC Currently Depressed)': 'Depress',
        })
    elif project == 'REMBRANDT':
        # Use arm/events names to determine which arm
        df['GROUP'] = df['redcap_event_name'].map({
            'Screening (Arm 3: Longitudinal Phase: Remitted)': 'Depress',
            'Screening (Arm 2: Longitudinal Phase: Never Depressed)': 'Control',
        })

    # Load MRI records to get first date
    if dob_field and date_field:
        fields = [def_field, date_field]
        rec = project_redcap.export_records(fields=fields, raw_or_label='label')
        rec = [x for x in rec if x[date_field]]
        dfm = pd.DataFrame(rec, columns=fields)
        dfm = dfm.astype(str)
        dfm = dfm.sort_values(date_field)
        dfm = dfm.drop_duplicates(subset=[def_field], keep='first')

        # Merge in date
        df = pd.merge(df, dfm, how='left', on=def_field)

        df[sex_field] = df[sex_field].fillna('UNKNOWN')

        # Exclude incomplete data
        df = df.dropna()

        # Calculate age at baseline
        df[dob_field] = pd.to_datetime(df[dob_field])
        df[date_field] = pd.to_datetime(df[date_field])
        df['AGE'] = (
            df[date_field] - df[dob_field]
        ).values.astype('<m8[Y]').astype('int').astype('str')

        if include_dob:
            df['DOB'] = df[dob_field]

    # Exclude incomplete data
    df = df.dropna()

    if sex_field:
        df['SEX'] = df[sex_field].map({
            'Male': 'M',
            'Female': 'F',
            'UNKNOWN': 'U'
        })

    if guid_field:
        df['GUID'] = df[guid_field]

    if sec_field:
        df['ID'] = df[sec_field]
    else:
        df['ID'] = df[def_field]

    # Drop intermediate columns
    drop_columns = [def_field, 'redcap_event_name', dob_field, date_field, sec_field, guid_field, sex_field]
    drop_columns = [x for x in drop_columns if x and x in df.columns]
    df = df.drop(columns=drop_columns)

    # Finish up
    df = df.sort_values('ID')
    df = df.drop_duplicates()
    df = df.set_index('ID')

    return df
