import json
import logging
import glob

import pandas as pd
from garjus.utils_redcap import get_redcap, upload_file
from garjus import Garjus



# TODO: def update_assessor that only imports differences
# Get subjects from study anon project, load to Taylor anon project
# TODO: copy processors and analyses from existing redcap
# TODO: set complete according to status?
# TODO: batch import scans/assessors for session
# TODO: upload files EDAT
# def import_stat, batch import per assessor
# TODO: Load data from garjus for scans and assessors, later from NIFTI/BIDS?
# TODO: Filter subjects to only with imaging
# TODO: also check for existing assessor with same (proctype,inputs) to enforce uniqueness
# def load_DepMIND3():
# def load_DepMIND2()
# project = '225711'


logger = logging.getLogger('garjus')


COLUMNS = ['ID', 'AGE', 'SEX', 'GROUP']

SESSION_MAP = {
    'D3':
    {
        'a': 'Baseline', 
        'b': 'EndStep1'
    },
    'REMBRANDT':
    {
        'a': 'Baseline', 
        'b': 'Month08',
        'c': 'Month16',
        'd': 'Month24',
        'e': 'Triggered',
    },
    'GNB':
    {
        'a': 'Baseline', 
    },
    'R21Perfusion':
    {
        'a': 'Baseline',
    },
    'DepMIND1':
    {
        'a': 'Baseline',
        'b': 'Week12',
    },
     'DepMIND2':
    {
        'a': 'Baseline',
        'b': 'Week06',
        'c': 'Week12',
    },
     'DepMIND3':
    {
        'a': 'Baseline',
        'b': 'Week06',
    },
    'CAARE': {
        'a': 'Baseline',
        'b': 'Month08',
    },
}


def load_REMBRANDT_subjects():
    project = 'REMBRANDT'
    project_id = '232588'
    rc_anon = get_redcap(project_id=project_id)
    def_field = rc_anon.def_field
    fields = ['anon_id', 'age', 'sex_xcount']

    records = rc_anon.export_records(fields=fields)

    df = pd.DataFrame(records)
    df = df[~df.anon_id.str.startswith('a')]
    df = df[df.anon_id != '']

    df['ID'] =  df['anon_id']
    df['AGE'] =  df['age']
    df['SEX'] =  df['sex_xcount'].map({'0': 'M', '1': 'F'}).fillna('')
    df['GROUP'] = df['redcap_event_name'].map({
        'screening_arm_2':'CONTROL',
        'screening_arm_3': 'DEPRESS'
    })

    df = df[COLUMNS]
    df['PROJECT'] =  'REMBRANDT'

    return df


def load_D3_subjects():
    project = 'D3'
    project_id = '227608'
    rc_anon = get_redcap(project_id=project_id)
    def_field = rc_anon.def_field
    fields = ['anon_id', 'age', 'sex_xcount']

    records = rc_anon.export_records(fields=fields)

    df = pd.DataFrame(records)
    df = df[df.anon_id != '']

    df['ID'] =  df['anon_id']
    df['AGE'] =  df['age']
    df['SEX'] =  df['sex_xcount'].map({'0': 'M', '1': 'F'}).fillna('')
    df['GROUP'] = df['redcap_event_name'].map({
        'screening_arm_1': 'DEPRESS',
        'screening_arm_2': 'CONTROL',
        'screening_arm_3': 'DEPRESS',
        'screening_arm_4': 'CONTROL',
    }).fillna('')
    df = df[df.GROUP != '']
    df = df[COLUMNS]
    df['PROJECT'] =  'D3'

    return df


def current_assessors(rc, project):
    records = rc.export_records(fields=[rc.def_field, 'assr_id'])
    return [x['assr_id'] for x in records if (x['assr_id'] and x[rc.def_field] == project)]


def current_scans(rc, project):
    records = rc.export_records(fields=[rc.def_field, 'scan_session', 'scan_id'])
    return [(x['scan_session'],x['scan_id']) for x in records if (x['scan_id'] and x[rc.def_field] == project)]


def current_subjects(rc, project):
    records = rc.export_records(fields=[rc.def_field, 'subject_id'])
    subjects = [x['subject_id'] for x in records if (x['subject_id'] and x[rc.def_field] == project)]
    return subjects


def current_sessions(rc, project):
    records = rc.export_records(fields=[rc.def_field, 'session_id'])
    sessions = [x['session_id'] for x in records if (x['session_id'] and x[rc.def_field] == project)]
    return sessions


def current_edats(rc, project):
    records = rc.export_records(fields=[rc.def_field, 'edat_sourcepath'])
    edats = [x['edat_sourcepath'] for x in records if (x['edat_sourcepath'] and x[rc.def_field] == project)]
    return edats


def load_anon_subjects(rc, project):
    records = rc.export_records(fields=[rc.def_field], records=[project], forms=['subjects'])
    records = [x for x in records if x['subject_id']]
    return records 


def anon_edats(rc, project):
    records = rc.export_records(fields=[rc.def_field], records=[project], forms=['edats'])
    records = [x for x in records if x['edat_sourcepath']]
    return records


def import_assessor(rc, project_id, assessor):
    # check for session/subject parent exists

    # check for existing, to enforce uniqueness
    if assessor['ID'] in current_assessors(rc, project_id):
        raise Exception('failed to import, assessor exists')

    record = {
        rc.def_field: project_id,
        'assr_id': assessor['ID'],
        'assr_subject': assessor['SUBJECT'],
        'assr_session': assessor['SESSION'],
        'assr_procstatus': assessor['PROCSTATUS'],
        'assr_proctype': assessor['PROCTYPE'],
        'assr_jobdate': assessor['JOBDATE'],
        'assr_timeused': assessor['TIMEUSED'],
        'assr_memused': assessor['MEMUSED'],
        'assr_timereq': assessor['WALLTIME'],
        'assr_memreq': assessor['MEMREQ'],
        'assr_jobnode': assessor['JOBNODE'],
        'assr_qcstatus': assessor['QCSTATUS'],
        'assr_qcdate': assessor['QCDATE'],
        'assr_qcby': assessor['QCBY'],
        'assr_failcount': assessor['FAILCOUNT'],
        'assr_userinputs': assessor['USERINPUTS'],
        'assr_inputs': assessor['INPUTS'],
        'redcap_repeat_instrument': 'assessors',
        'redcap_repeat_instance': 'new',
        'assessors_complete': '2',
    }

    print(f'importing record:{record}')
    try:
        response = rc.import_records([record])
        assert 'count' in response
    except Exception as err:
        print(f'ERROR:uploading assessor:{err}')


def import_session(rc, project_id, session, check=True):
    # check for existing, to enforce uniqueness
    if check and session['ID'] in current_sessions(rc, project_id):
        raise Exception('failed to import, session exists')

    record = {
        rc.def_field: project_id,
        'session_id': session['ID'],
        'session_subject': session['SUBJECT'],
        'session_sesstype': session['SESSTYPE'],
        'session_tracer': session['TRACER'],
        'session_note': session['NOTE'],
        'session_modality': session['MODALITY'],
        'redcap_repeat_instrument': 'sessions',
        'redcap_repeat_instance': 'new',
        'sessions_complete': '2',
    }

    try:
        response = rc.import_records([record])
        assert 'count' in response
    except Exception as err:
        print(f'ERROR:uploading session:{err}')


def import_scan(rc, project_id, scan, check=True):
    # check for existing, to enforce uniqueness
    if check and (scan['SESSION'], scan['SCANID']) in current_scans(rc, project_id):
        raise Exception('failed to import, scan exists')

    record = {
        rc.def_field: project_id,
        'scan_id': scan['SCANID'],
        'scan_session': scan['SESSION'],
        'scan_scantype': scan['SCANTYPE'],
        'scan_descrip': scan['SCANDESCRIP'],
        'scan_frames': scan.get('FRAMES', ''),
        'scan_quality': scan.get('QUALITY', ''),
        'scan_resources': scan.get('RESOURCES', ''),
        'scan_duration': scan.get('DURATION', ''),
        'scan_tr': scan.get('TR', ''),
        'scan_prfip': scan.get('SENSE', ''),
        'scan_proop': scan.get('MB', ''),
        'scan_slicethickness': scan.get('THICK', ''),
        'redcap_repeat_instrument': 'scans',
        'redcap_repeat_instance': 'new',
        'scans_complete': '2',
    }

    try:
        response = rc.import_records([record])
        assert 'count' in response
    except Exception as err:
        print(f'ERROR:uploading scan:{err}')


def import_subject(rc, project_id, subject, check=True):
    # check for existing, to enforce uniqueness of subject ID
    if check and subject['ID'] in current_subjects(rc, project_id):
        raise Exception('failed to import, subject exists')

    record = {
        rc.def_field: project_id,
        'subject_id': subject['ID'],
        'subject_age': subject['AGE'],
        'subject_group': subject['GROUP'],
        'subject_sex': subject['SEX'],
        'redcap_repeat_instrument': 'subjects',
        'redcap_repeat_instance': 'new',
        'subjects_complete': '2',
    }

    try:
        response = rc.import_records([record])
        assert 'count' in response
    except Exception as err:
        print(f'ERROR:uploading subject:{err}')


def load_test(rc):

    subject = {
        'ID': '99999',
        'AGE': '999',
        'SEX': 'M',
        'GROUP': 'UNKNOWN',
    }

    scan = {
        'SCANID': '301',
        'SESSION': '9999a',
        'SCANTYPE': 'T1',
        'FRAMES': '160',
        'QUALITY': 'usable',
        'RESOURCES': 'NIFTI,JSON',
        'DURATION': '300',
        'TR': '6.8',
        'SENSE': '2',
        'MB': '1',
        'THICK': '1.0',
    }

    session = {
        'ID': '9999a',
        'SUBJECT': '99999',
        'SESSTYPE': 'Baseline',
        'TRACER': '',
        'NOTE': 'No Resting State',
        'MODALITY': 'MR',
    }

    assessor = {
        'ID': 'D3-x-99999-x-99999a-x-FS7_v1-x-9999',
        'SUBJECT': '99999',
        'SESSION': '9999a',
        'PROCSTATUS': 'COMPLETE',
        'PROCTYPE': 'FS7_v1',
        'JOBDATE': '2026-01-01',
        'TIMEUSED': '12:24', 
        'MEMUSED': '3GB',
        'JOBNODE': 'vampire.334',
        'QCSTATUS': 'Passed',
        'QCDATE': '2026-07-01',
        'QCBY': 'boydb1',
        'INPUTS': 'scan_t1: 301',
    }

    try:
        import_subject(rc, 'D3', subject)
    except:
        pass

    try:
        import_scan(rc, 'D3', scan)
    except:
        pass

    try:
        import_session(rc, 'D3', session)
    except:
        pass

    try:
        import_assessor(rc, 'D3', assessor)
    except:
        pass


def _scan_subjects(project):
    return Garjus().scans(projects=[project]).SUBJECT.astype(str).unique()


def import_D3_subjects(rc):
    '''Load subjects from Anon redcap'''
    df = load_D3_subjects()

    # Only import those with imaging data
    scan_subjects = _scan_subjects('TAYLOR_ANON')
    df = df[df.ID.astype(str).isin(scan_subjects)].reset_index(drop=True)

    cur_subjects = current_subjects(rc, 'D3')

    # Load each subject to REDCap
    for i, subject in df.iterrows():

        # check for existing
        if subject['ID'] in cur_subjects:            
            print(i, 'subject already imported', subject['ID'])
            continue

        try:
            import_subject(rc, 'D3', subject)
            print(i, 'created', subject['ID'])
        except:
            print(i, 'exists', subject['ID'])


def import_D3_sessions(rc):
    '''Load sessions from XNAT scans'''
    scans = Garjus().scans(projects=['TAYLOR_ANON'])
    scans = scans[scans.SUBJECT.str.startswith('18')]
    df = scans[['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'TRACER', 'MODALITY', 'NOTE']]
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    df = df.rename(columns={'SESSION': 'ID'})
    df['SESSTYPE'] = df.ID.str[-1].map(D3_SESSION_MAP)

    cur_subjects = current_subjects(rc, 'D3')
    cur_sessions = current_sessions(rc, 'D3')

    # Load each session to REDCap
    for i, session in df.iterrows():

        # check for existing SUBJECT
        if session['SUBJECT'] not in cur_subjects:
            raise Exception('failed to import session, subject does not exist')

        if session['ID'] in cur_sessions:            
            print(i, 'session already imported', session['ID'])
            continue

        try:
            import_session(rc, 'D3', session)
            print(i, 'created', session['ID'])
        except:
            print(i, 'exists', session['ID'])


def import_D3_scans(rc):
    '''Load XNAT scans'''
    scans = Garjus().scans(projects=['TAYLOR_ANON'])
    df = scans[scans.SUBJECT.str.startswith('18')].reset_index(drop=True)
    df = df.fillna('')
    cur_sessions = current_sessions(rc, 'D3')
    cur_scans = current_scans(rc, 'D3')

    # Load each session to REDCap
    for i, scan in df.iterrows():
        #if i > 0:
        #    continue

        # check for existing SUBJECT
        if scan['SESSION'] not in cur_sessions:
            raise Exception('failed to import scan, session does not exist')

        if (scan['SESSION'], scan['SCANID']) in cur_scans:            
            print(i, 'scan already imported', scan['SESSION'], scan['SCANID'])
            continue

        try:
            import_scan(rc, 'D3', scan)
            print(i, 'created', scan['SESSION'], scan['SCANID'])
        except:
            print(i, 'exists', scan['SESSION'], scan['SCANID'])


def import_D3_assessors(rc):
    '''Load XNAT assessors'''
    df = Garjus().assessors(projects=['TAYLOR_ANON'])
    df = df.rename(columns={'ASSR': 'ID'})
    df = df[df.SUBJECT.str.startswith('18')].reset_index(drop=True)
    df = df.fillna('')
    df = df[df.QCSTATUS != '']
    df.INPUTS = df.INPUTS.map(json.dumps)

    cur_sessions = current_sessions(rc, 'D3')
    cur_assessors = current_assessors(rc, 'D3')

    tasks = Garjus().tasks(projects=['TAYLOR_ANON'], hidedone=False)
    tasks = tasks[['ASSESSOR', 'WALLTIME', 'MEMREQ',  'FAILCOUNT', 'USERINPUTS']]
    tasks.USERINPUTS = tasks.USERINPUTS.map(json.dumps)
    tasks.USERINPUTS = tasks.USERINPUTS.replace({'"{}"'}, '')
    df = pd.merge(df, tasks, left_on='ID', right_on='ASSESSOR')

    # Load each session to REDCap
    for i, assr in df.iterrows():
        # check for no existing session
        if assr['SESSION'] not in cur_sessions:
            raise Exception('failed to import assessor, session does not exist')

        # check for existing assessor
        if assr['ID'] in cur_assessors:            
            print(i, 'assessor already imported', assr['ID'])
            continue

        try:
            import_assessor(rc, 'D3', assr)
            print(i, 'created', assr['ID'])
        except:
            print(i, 'exists', assr['ID'])


def import_D3_edats(rc):
    pass


def import_D3(rc):
    import_D3_subjects(rc)
    import_D3_sessions(rc)
    import_D3_scans(rc)
    import_D3_assessors(rc)


def import_path(rc, path):
    proj = subj = sess = scan_id = ''

    try:
        if path.startswith('./D3') or path.startswith('./GNB'):
            _, proj, site, subj, sess, scan, file = path.split('/')
            scan_id, scan_type, scan_descrip = scan.split('-x-')
        else:
            _, proj, subj, sess, scan, file = path.split('/')
            scan_id, scan_type, scan_descrip = scan.split('-x-')
    except:
        print(f'NOPE!:{path}')
        pass

    if proj != 'D3':
        return

    # check for existing, to enforce uniqueness
    if (sess, scan_id) in current_scans(rc, proj):
        return

    print(f'import:{proj=},{subj=},{sess=},{scan_id=},{scan_type=},{scan_descrip=}')

    record = {
        rc.def_field: proj,
        'scan_id': scan_id,
        'scan_session': sess,
        'scan_scantype': scan_type,
        'scan_descrip': scan_descrip,
        'redcap_repeat_instrument': 'scans',
        'redcap_repeat_instance': 'new',
        'scans_complete': '2',
    }

    try:
        response = rc.import_records([record])
        assert 'count' in response
    except Exception as err:
        print(f'ERROR:uploading scan:{err}')


def _parse(filepath):
    record = {}

    try:
        if filepath.startswith('./D3') or filepath.startswith('./GNB'):
            _, proj, site, subj, sess, scan, file = filepath.split('/')
            scan_id, scan_type, scan_descrip = scan.split('-x-')
        else:
            _, proj, subj, sess, scan, file = filepath.split('/')
            scan_id, scan_type, scan_descrip = scan.split('-x-')

        record = {
            'PROJECT': proj,
            'SUBJECT': subj,
            'SCANID': scan_id,
            'SESSION': sess,
            'SCANTYPE': scan_type,
            'SCANDESCRIP': scan_descrip,
            'SCANPATH': filepath,
        }
    except:
        print(f'NOPE!:{filepath}')

    return record


def parse_paths(paths):
    data = []

    jsons = sorted([x for x in paths if x.endswith('.json')])
    niftis = sorted([x for x in paths if x.endswith('.nii.gz')])
    bvals = sorted([x for x in paths if x.endswith('.bval')])
    bvecs = sorted([x for x in paths if x.endswith('.bvec')])
    logs = sorted([x for x in paths if x.endswith('d2n-log.txt')])
    junk = sorted([x for x in paths if x.endswith('.DS_Store')])

    print(f'{len(paths)} total paths')
    print(f'{len(jsons)} jsons')
    print(f'{len(niftis)} niftis')
    print(f'{len(bvals)} bvals')
    print(f'{len(bvecs)} bvecs')
    print(f'{len(logs)} logs')
    print(f'{len(junk)} junk')
    _others = len(paths) - len(jsons) - len(niftis) - len(bvals) - len(bvecs) - len(logs) - len(junk)
    print(f'{_others} others')

    for j in jsons:
        record = _parse(j)
        data.append(record)

    return data


def load_paths(filename):
    paths = []

    print(f'read {filename}')
    with open(filename, 'r') as f:
        paths = [line.rstrip() for line in f]

    return paths


def import_scans(rc, df):
    for i, scan in df.iterrows():
        project_id = scan['PROJECT']
        print(i, scan['SCANPATH'])
        import_scan(rc, project_id, scan, check=False)


def import_sessions(rc, df):
    for i, session in df.iterrows():
        project_id = session['PROJECT']
        session['SESSTYPE'] = session.get('SESSTYPE', '')
        session['TRACER'] = session.get('TRACER', '')
        session['NOTE'] = session.get('NOTE', '')
        session['MODALITY'] = session.get('MODALITY', '')
        print(i, session['ID'])
        import_session(rc, project_id, session, check=False)


def import_subjects(rc, df):
    for i, subject in df.iterrows():
        project_id = subject['PROJECT']
        print(i, subject['ID'])
        subject['AGE'] = subject.get('AGE', '')
        subject['GROUP'] = subject.get('GROUP', '')
        subject['SEX'] = subject.get('SEX', '')
        import_subject(rc, project_id, subject, check=False)


def import_all_scans(rc, df):
    for proj in df.PROJECT.dropna().unique():
        try:
            df_proj = df[df.PROJECT == proj]
            cur_scans = current_scans(rc, proj)
            df_proj = df_proj[[x not in cur_scans for x in zip(df_proj['SESSION'], df_proj['SCANID'])]]
        except:
            print(f'{proj}:no scans')
            continue

        if len(df_proj) == 0:
            print(f'{proj}:all done')
            continue

        print(f'{proj}:{len(df_proj)} records to import')
        import_scans(rc, df_proj)


def import_all_sessions(rc, df):
    for proj in df.PROJECT.dropna().unique():
        try:
            df_proj = df[df.PROJECT == proj]
            cur_sessions = current_sessions(rc, proj)
            df_proj = df_proj[[x not in cur_sessions for x in df_proj['ID']]]
        except:
            print(f'{proj}:no sessions')
            continue

        if len(df_proj) == 0:
            print(f'{proj}:all done')
            continue

        if proj in SESSION_MAP:
            df_proj['SESSTYPE'] = df_proj.ID.str[-1].map(SESSION_MAP.get(proj)).fillna('')

        print(f'{proj}:{len(df_proj)} records to import')
        import_sessions(rc, df_proj)


def import_all_subjects(rc, df):
    for proj in df.PROJECT.dropna().unique():
        try:
            df_proj = df[df.PROJECT == proj]
            cur_subjects = current_subjects(rc, proj)
            df_proj = df_proj[[x not in cur_subjects for x in df_proj['ID']]]
        except:
            print(f'{proj}:no subjects')
            continue

        if len(df_proj) == 0:
            print(f'{proj}:all done')
            continue

        print(f'{proj}:{len(df_proj)} records to import')
        import_subjects(rc, df_proj)


def update_subjects(rc, project_id):
    # Load from D3 anon study main redcap database
    if project_id == 'D3':
        main_subjects = load_D3_subjects()
    elif project_id == 'REMBRANDT':
        main_subjects = load_REMBRANDT_subjects()
    else:
        raise('nope')

    # Load subjects from combined db
    df = pd.DataFrame(load_anon_subjects(rc, project_id))

    # Only those with imaging data
    cur_subjects = current_subjects(rc, project_id)
    main_subjects = main_subjects[main_subjects.ID.astype(str).isin(cur_subjects)].reset_index(drop=True)

    # Update each source subject to destination in combined redcap
    for i, main_subj in main_subjects.iterrows():
        # Match with destination subject
        for j, dest_subj in df.iterrows():
            if str(main_subj['ID']) == str(dest_subj['subject_id']):
                # Matches
                update_subject(rc, main_subj, dest_subj)


def update_subject(rc, src_subject, dst_subject):
    record = {}

    if src_subject['ID'] != dst_subject['subject_id']:
        raise 'nope'

    if src_subject['AGE'] and not dst_subject['subject_age']:
        record['subject_age'] = src_subject['AGE']

    if src_subject['SEX'] and not dst_subject['subject_sex']:
        record['subject_sex'] = src_subject['SEX']

    if src_subject['GROUP'] and not dst_subject['subject_group']:
        record['subject_group'] = src_subject['GROUP']

    if not record:
        # Nothing to update
        return

    # Add record identifying fields
    record[rc.def_field] = dst_subject[rc.def_field]
    record['redcap_repeat_instrument'] = dst_subject['redcap_repeat_instrument']
    record['redcap_repeat_instance'] = dst_subject['redcap_repeat_instance']
    print(f'updating subject:src={src_subject.ID}, dst={dst_subject.subject_id}, {record=}')  
    try:
        response = rc.import_records([record])
        assert 'count' in response
    except Exception as err:
        print(f'ERROR:updating subject:{err}')


def upload_all_edats(rc, df):

    for proj in df.PROJECT.dropna().unique():
        try:
            df_proj = df[df.PROJECT == proj]
            df_proj = df_proj[[x not in cur_subjects for x in df_proj['ID']]]
        except:
            print(f'{proj}:no subjects')
            continue

        if len(df_proj) == 0:
            print(f'{proj}:all done')
            continue

        print(f'{proj}:{len(df_proj)} records to import')
        update_edats(rc, df_proj, df_proj)


def upload_edats(rc, project_id, edats):

    # Update each edat combined redcap
    for i, edat in edats.iterrows():
        upload_edat(rc, project_id, edat)


def upload_edat(rc, project_id, edat):
    # TODO check for existing, to enforce uniqueness

    # Create record for upload
    record = {
        rc.def_field: project_id,
        'edat_sourcepath': edat['edat_sourcepath'],
        'edat_session': edat['edat_session'],
        'edat_tasktype': edat['edat_tasktype'],
        'redcap_repeat_instrument': 'edats',
        'redcap_repeat_instance': 'new',
        'edats_complete': '2',
    }

    try:
        response = rc.import_records([record])
        assert 'count' in response
    except Exception as err:
        print(f'ERROR:uploading edat:{err}')

    # Match new record and upload file
    df = pd.DataFrame(anon_edats(rc, project_id))
    match_found = False
    for j, dest in df.iterrows():
        if str(record['edat_sourcepath']) == str(dest['edat_sourcepath']):
            # Matches so upload to this record
            match_found = True
            print(f'uploading:{record[rc.def_field]}:{dest["redcap_repeat_instance"]}:{edat["edat_sourcepath"]}')
            result = upload_file(
                rc,
                record[rc.def_field],
                'edat_tabfile',
                edat['edat_sourcepath'],
                repeat_id=dest['redcap_repeat_instance']
            )
            # finish the loop so we don't match again
            break

    if not match_found:
        raise Exception(f'failed to match record for uploading file:{record}')


def load_edats(edat_dir):
    data = []

    paths = sorted(glob.glob(f'{edat_dir}/*edat2tab.txt'))

    for p in paths:
        session, tasktype = p.split('/')[-1].replace('_edat2tab.txt', '').split('_', 1)
        data.append({
            'edat_sourcepath': p,
            'edat_session': session,
            'edat_tasktype': tasktype,
        })

    return pd.DataFrame(data)


# Load data
rc = get_redcap(project_id='233188')
paths = load_paths('/Users/boydb1/Desktop/TAYLOR_ANON-filepaths.txt')
data = parse_paths(paths)
df = pd.DataFrame(data)

# Import row per scan
if False:
    import_all_scans(rc, df)

# Get unique sessions
df = df[['PROJECT', 'SUBJECT', 'SESSION']]
df = df.drop_duplicates()
df = df.reset_index(drop=True)
df = df.rename(columns={'SESSION': 'ID'})
if False:
    import_all_sessions(rc, df)

# Get unique subjects
df = df[['PROJECT', 'SUBJECT']]
df = df.drop_duplicates()
df = df.reset_index(drop=True)
df = df.rename(columns={'SUBJECT': 'ID'})
if False:
    import_all_subjects(rc, df)


# TODO: update sessions to add session type and modality where missing
# TODO: update scans to add values extracted from json where missing
# TODO: update scans to upload json file where missing
if False:
    update_subjects(rc, 'D3')
    update_subjects(rc, 'REMBRANDT')

#update_edats(rc, 'D3')
#edats = load_edats('/Users/boydb1/D3_EDAT_2026-09-01/fMRI_MIDT1/D3/D3_ANON-MIDT_EDAT-2026-09-03')
#upload_edats(rc, 'D3', edats)
#edats = load_edats('/tmp/EDAT_ANON/CAARE_ANON-EDP_EDAT-2026-09-03')
#upload_edats(rc, 'CAARE', edats)
#edats = load_edats('/tmp/EDAT_ANON/D3_ANON-EEFRT_EDAT-2026-09-03')
#print(f'found {len(edats)} edats')
#upload_edats(rc, 'D3', edats)
#edats = load_edats('/tmp/EDAT_ANON/DepMIND3_ANON-EmoStroop_EDAT-2026-09-03')
#print(f'found {len(edats)} edats')
#upload_edats(rc, 'DepMIND3', edats)
#edats = load_edats('/tmp/EDAT_ANON/REMBRANDT_ANON-MSIT_EDAT-2026-09-02')
#cur_edats = current_edats(rc, 'REMBRANDT')
#edats = edats[~edats.edat_sourcepath.isin(cur_edats)]
#print(f'found {len(edats)} edats')
#upload_edats(rc, 'REMBRANDT', edats)

print('DONE!')
