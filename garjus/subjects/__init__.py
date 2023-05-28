'''Subjects from REDCap.'''
import pandas as pd


def load_subjects(garjus, project, include_dob=False):
    project_redcap = garjus.primary(project)
    
    if not project_redcap:
        return pd.DataFrame([], columns=['ID', 'PROJECT'])

    def_field = project_redcap.def_field
    sec_field = project_redcap.export_project_info()['secondary_unique_field']
    guid_field = None
    sex_field = None
    dob_field = None
    date_field = None
    field_names = project_redcap.field_names

    if 'guid' in field_names:
        guid_field = 'guid'

    if 'dob' in field_names:
        dob_field = 'dob'
    elif 'dob_sub' in field_names:
        dob_field = 'dob_sub'

    if 'sex_xcount' in field_names:
        sex_field = 'sex_xcount'
    elif 'dems_sex' in field_names:
        sex_field = 'dems_sex'
    elif 'sex_demo' in field_names:
        sex_field = 'sex_demo'

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
        rec = [x for x in rec if x[dob_field]]

    # Make data frame
    if project_redcap.is_longitudinal:
        df = pd.DataFrame(rec, columns=fields+['redcap_event_name'])
    else:
        df = pd.DataFrame(rec, columns=fields)

    # Set the project
    df['PROJECT'] = project

    # Determine group
    df['GROUP'] = 'UNKNOWN'

    if project == 'DepMIND2':
        # All DM2 are depressed
        df['GROUP'] = 'Depress'
    elif project == 'D3':
        # Use arm/events names to determine group
        df['GROUP'] = df['redcap_event_name'].map({
            'Screening (Arm 2: Never Depressed)': 'Control',
            'Screening (Arm 1: Currently Depressed)': 'Depress',
        })
    elif project == 'REMBRANDT':
         # Use arm/events names to determine which arm
        df['GROUP'] = df['redcap_event_name'].map({
            'Screening (Arm 3: Longitudinal Phase: Remitted)': 'Depress',
            'Screening (Arm 2: Longitudinal Phase: Never Depressed)':  'Control',
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

        # Exclude incomplete data
        df = df.dropna()

        # Calculate age at baseline
        df[dob_field] = pd.to_datetime(df[dob_field])
        df[date_field] = pd.to_datetime(df[date_field])
        df['AGE'] = (
            df[date_field]  - df[dob_field]
        ).values.astype('<m8[Y]').astype('int').astype('str')

        if include_dob:
            df['DOB'] = df[dob_field]

    # Exclude incomplete data
    df = df.dropna()

    if sex_field:
        df['SEX'] = df[sex_field].map({'Male': 'M', 'Female': 'F'})

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
