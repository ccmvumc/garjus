import os
import logging

import redcap
import pandas as pd


def download_named_file(
    project,
    record_id,
    field_id,
    outdir,
    event_id=None,
    repeat_id=None
):
    # Get the file contents from REDCap
    try:
        (cont, hdr) = project.export_file(
            record=record_id,
            field=field_id,
            event=event_id,
            repeat_instance=repeat_id)

        if cont == '':
            raise redcap.RedcapError
    except redcap.RedcapError as err:
        logging.error(f'downloading file:{err}')
        return None

    # Save contents to local file
    filename = os.path.join(outdir, hdr['name'])
    try:
        with open(filename, 'wb') as f:
            f.write(cont)

        return filename
    except FileNotFoundError as err:
        logging.error(f'file not found:{filename}:{err}')
        return None


def download_file(
    project,
    record_id,
    field_id,
    filename,
    event_id=None,
    repeat_id=None
):
    # Get the file contents from REDCap
    try:
        (cont, hdr) = project.export_file(
            record=record_id,
            field=field_id,
            event=event_id,
            repeat_instance=repeat_id)

        if cont == '':
            raise redcap.RedcapError
    except redcap.RedcapError as err:
        logging.error(f'downloading file:{err}')
        return None

    # Save contents to local file
    try:
        with open(filename, 'wb') as f:
            f.write(cont)

        return filename
    except FileNotFoundError as err:
        logging.error(f'file not found:{filename}:{err}')
        return None


def upload_file(
    project,
    record_id,
    field_id,
    filename,
    event_id=None,
    repeat_id=None
):
    with open(filename, 'rb') as f:
        return project.import_file(
            record=record_id,
            field=field_id,
            file_name=os.path.basename(filename),
            event=event_id,
            repeat_instance=repeat_id,
            file_object=f)


def get_redcap(project_id=None, key_file=None, api_url=None, api_key=None):
    # Check for overrides in environment vars
    api_url = os.environ.get('REDCAP_API_URL', api_url)
    key_file = os.environ.get('REDCAP_API_KEYFILE', key_file)

    if not api_url:
        api_url = 'https://redcap.vumc.org/api/'

    if not api_key:
        # key not specified so we read it from file

        if not key_file:
            # no key file specified so we use the default location
            key_file = os.path.join(os.path.expanduser('~'), '.redcap.txt')

        # Load from the key file
        if project_id:
            api_key = get_projectkey(project_id, key_file)

    if not api_key:
        raise Exception('api key not found in file or arguments')

    return redcap.Project(api_url, api_key)


def get_projectkey(project_id, key_file):
    # Load the dictionary
    d = {}
    with open(key_file) as f:
        for line in f:
            if line == '':
                continue

            try:
                (i, k, n) = line.strip().split(',')
                d[i] = k
            except Exception:
                pass

    # Return the key id for given project id
    return d.get(project_id, None)


def get_projectid(projectname, keyfile):
    # Load the dictionary mapping name to id
    d = {}
    with open(keyfile) as f:
        for line in f:
            if line == '':
                continue
            try:
                (i, k, n) = line.strip().split(',')
                # Map name to id
                d[n] = i
            except Exception:
                pass
    # Return the project id for given project name
    return d.get(projectname, None)


def get_main_redcap():
    api_url = 'https://redcap.vumc.org/api/'
    keyfile = os.path.join(os.path.expanduser('~'), '.redcap.txt')

    # Check for overrides in environment vars
    api_url = os.environ.get('REDCAP_API_URL', api_url)
    keyfile = os.environ.get('REDCAP_API_KEYFILE', keyfile)

    project_id = get_projectid('main', keyfile)
    api_key = get_projectkey(project_id, keyfile)

    if not api_key:
        return None

    return redcap.Project(api_url, api_key)


def get_rcq_redcap():
    api_url = 'https://redcap.vumc.org/api/'
    keyfile = os.path.join(os.path.expanduser('~'), '.redcap.txt')

    # Check for overrides in environment vars
    api_url = os.environ.get('REDCAP_API_URL', api_url)
    keyfile = os.environ.get('REDCAP_API_KEYFILE', keyfile)

    project_id = get_projectid('rcq', keyfile)
    api_key = get_projectkey(project_id, keyfile)

    if not api_key:
        return None

    return redcap.Project(api_url, api_key)


def get_identifier_redcap():
    api_url = 'https://redcap.vumc.org/api/'
    keyfile = os.path.join(os.path.expanduser('~'), '.redcap.txt')

    # Check for overrides in environment vars
    api_url = os.environ.get('REDCAP_API_URL', api_url)
    keyfile = os.environ.get('REDCAP_API_KEYFILE', keyfile)

    project_id = get_projectid('identifier', keyfile)
    api_key = get_projectkey(project_id, keyfile)

    return redcap.Project(api_url, api_key)


def match_repeat(rc, record_id, repeat_name, match_field, match_value):
    # Load potential matches
    records = rc.export_records(records=[record_id])

    # Find records with matching vaue
    matches = [x for x in records if x[match_field] == match_value]

    # Return ids of matches
    return [x['redcap_repeat_instance'] for x in matches]


def field2events(project, field_id):
    events = []

    try:
        _form = [x['form_name'] for x in project.metadata if x['field_name'] == field_id][0]
        events = [x['unique_event_name'] for x in project.export_instrument_event_mappings() if x['form'] == _form]
    except IndexError:
        events = []

    return events


def secondary_map(project):
    def_field = project.def_field
    sec_field = secondary(project)

    if not sec_field:
        return {}

    # Get the secondary values
    rec = project.export_records(fields=[def_field, sec_field])

    # Build the map
    id2subj = {x[def_field]: x[sec_field] for x in rec if x.get(sec_field, False)}

    return id2subj


def secondary(project):
    return project.export_project_info()['secondary_unique_field']


# Load the table that links old/new id/date
def load_link(rc_pre, rc_anon, delete_dates=False):
    dfp = rc_pre.export_records(fields=['anon_id'])

    # Get old ID mapped to anon id from pre redcap project
    dfp = pd.DataFrame(dfp)
    dfp['ID'] = dfp[rc_pre.def_field].map(secondary_map(rc_pre))
    dfp = dfp[dfp.anon_id != '']
    dfp = dfp[['ID', 'anon_id']]
    dfp['ID'] = dfp['ID'].astype(str)
    dfp['anon_id'] = dfp['anon_id'].astype(str)

    if delete_dates:
        df = dfp
    else:
        mri_date_field = ''

        if 'mri_date' in rc_pre.field_names:
            mri_date_field = 'mri_date'
        elif 'mriscan_date' in rc_pre.field_names:
            mri_date_field = 'mriscan_date'
        else:
            raise Exception('failed to find mri date in REDCap projects')

        # Load dates from both redcaps
        dfd = rc_pre.export_records(fields=[mri_date_field])
        dfa = rc_anon.export_records(fields=[mri_date_field])

        # Get old ID with old date from pre redcap project
        dfd = pd.DataFrame(dfd)
        dfd['ID'] = dfd[rc_pre.def_field].map(secondary_map(rc_pre))
        dfd = dfd[dfd[mri_date_field] != '']
        dfd['ID'] = dfd['ID'].astype(str)

        if rc_pre.is_longitudinal:
            dfd = dfd[['ID', 'redcap_event_name', mri_date_field]]
        else:
            dfd = dfd[['ID', mri_date_field]]

        # Get anon_id with anon date from anon redcap project
        dfa = pd.DataFrame(dfa)
        dfa['anon_id'] = dfa[rc_anon.def_field].map(secondary_map(rc_anon))
        dfa = dfa[dfa[mri_date_field] != '']
        dfa['anon_id']  = dfa['anon_id'].astype(str)

        if rc_pre.is_longitudinal:
            dfa = dfa[['anon_id', 'redcap_event_name', mri_date_field]]
        else:
            dfa = dfa[['anon_id', mri_date_field]]

        dfa = dfa.rename(columns={mri_date_field: 'anon_date'})

        # Merge all together to get one row per mri with both ids and both dates
        df = pd.merge(dfp, dfd, on='ID')

        if rc_pre.is_longitudinal:
            df = pd.merge(df, dfa, on=['anon_id', 'redcap_event_name'])
        else:
            df = pd.merge(df, dfa, on=['anon_id'])

    # Final sort
    df = df.sort_values('ID')

    return df
