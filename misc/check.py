import sys, os
from glob import glob

import pandas as pd
import pydicom
import garjus


# Load the table that links old/new id/date
def load_link(rc_pre, rc_anon):
    dfd = rc_pre.export_records(fields=['mri_date'])
    dfp = rc_pre.export_records(fields=['anon_id'])
    dfa = rc_anon.export_records(fields=['mri_date'])

    # Get old ID with old date from pre redcap project
    dfd = pd.DataFrame(dfd)
    dfd['ID'] = dfd[rc_pre.def_field].map(garjus.utils_redcap.secondary_map(rc_pre))
    dfd = dfd[dfd.mri_date != '']
    dfd = dfd[['ID', 'redcap_event_name', 'mri_date']]

    # Get old ID mapped to anon id from pre redcap project
    dfp = pd.DataFrame(dfp)
    dfp['ID'] = dfp[rc_pre.def_field].map(garjus.utils_redcap.secondary_map(rc_pre))
    dfp = dfp[dfp.anon_id != '']
    dfp = dfp[['ID', 'anon_id']]

    # Get anon_id with anon date from anon redcap project
    dfa = pd.DataFrame(dfa)
    dfa['anon_id'] = dfa[rc_anon.def_field].map(garjus.utils_redcap.secondary_map(rc_anon))
    dfa = dfa[dfa.mri_date != '']
    dfa = dfa[['anon_id', 'redcap_event_name', 'mri_date']]
    dfa = dfa.rename(columns={'mri_date': 'anon_date'})

    # Merge all together to get one row per mri with both ids and both dates
    df = pd.merge(dfp, dfd, on='ID')
    df = pd.merge(df, dfa, on=['anon_id', 'redcap_event_name'])
    df = df.sort_values('ID')

    return df


def check_dicom(in_path, value):
    d = pydicom.dcmread(in_path)
    matches = _check_dicom(d, value)
    if matches:
        print(matches)

    return matches


def _check_dicom(dicom, value, matches=[]):
    for cur in dicom:
        if cur.value == value:
            matches.append(f'{cur.keyword}:{cur.tag}')

        if cur.VR == "SQ":
            for c in cur.value:
                _check_dicom(c, value, matches=matches)

    return matches


def check_scan(out_dir, mri_date):
    for i, d in enumerate(sorted(os.listdir(out_dir))):
        out_dicom = f'{out_dir}/{i}.dcm'
        os.makedirs(os.path.dirname(out_dicom), exist_ok=True)
        check_dicom(out_dicom, mri_date)


def check_session(out_dir, mri_date):
    for scan in sorted(os.listdir(out_dir)):
        if scan.startswith('.'):
                continue

        scan_out_dir = f'{out_dir}/{scan}/DICOM'
        check_scan(scan_out_dir, mri_date)


def check_project(out_dir, df):
    for subject in sorted(os.listdir(out_dir)):
        if subject.startswith('.'):
                continue

        for session in sorted(os.listdir(f'{out_dir}/{subject}')):
            if session.startswith('.'):
                continue

            try:
                rec = df[df['anon_id'] == subject].iloc[0]
            except Exception as err:
                print(f'No match found for subject:{subject}:{err}')
                continue

            mri_date = f'{rec["mri_date"]}'
            sess_out_dir = f'{out_dir}/{subject}/{session}'
            check_session(sess_out_dir, mri_date)

    print('Finished checking project.')


if __name__ == '__main__':
    # Get top-level directory from command-line
    root_out_dir = sys.argv[1]

    # Get table of old id, new id, old date, new date
    print('Loading link from REDCap')
    rc_pre = garjus.utils_redcap.get_redcap('220310')
    rc_anon = garjus.utils_redcap.get_redcap('222564')
    df = load_link(rc_pre, rc_anon)

    print('Checking anonymization to DICOM')
    check_project(root_out_dir, df)
