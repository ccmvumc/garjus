import sys, os
from glob import glob

import pandas as pd
import pydicom

from ..utils_redcap import secondary_map


# These tags are deleted in addition to all private tags
DELETE_FIELDS = [
    0x00101010,  # PatientsAge
    0x00100030,  # PatientBirthDate
    0x00101030,  # PatientWeight
    0x00100040,  # PatientSex
    0x00104000,  # PatientComments
    0x00080012,  # InstanceCreationDate
    0x00080018,  # SOP Instance UID
    0x00080013,  # InstanceCreationTime 
    0x00081111,  # Procedure
    0x00089092,  # Evidence
    0x0020000D,  # StudyInstanceUID
    0x0020000E,  # SeriesInstanceUID
    0x00200052,  # FrameReferenceUID
    0x00209221,  # DimensionOrganizationUID
    0x00209222,  # DimensionIndexUID
    0x00400254,  # PerformedProcedureStepDescription
    0x00400253,  # PerformedProcedureStepID
    0x00200010,  # Study ID
]

# These tags are replaced with the new shifted date
DATE_FIELDS = [
    0x00080023,  # ContentDate
    0x00080021,  # SeriesDate
    0x00080020,  # StudyDate
    0x00400244,  # PerformedProcedureStepStartDate
    0x00400250,  # PerformedProcedureStepEndDate
    0x00402004,  # IssueDateofImagingServiceReque
]

# These tags are replaced with the new shifted date and time
DATETIME_FIELDS = [
    0x0008002A  # AcquisitionDateTime
]


# Load the table that links old/new id/date
def load_link(rc_pre, rc_anon):
    dfd = rc_pre.export_records(fields=['mri_date'])
    dfp = rc_pre.export_records(fields=['anon_id'])
    dfa = rc_anon.export_records(fields=['mri_date'])

    # Get old ID with old date from pre redcap project
    dfd = pd.DataFrame(dfd)
    dfd['ID'] = dfd[rc_pre.def_field].map(secondary_map(rc_pre))
    dfd = dfd[dfd.mri_date != '']
    dfd = dfd[['ID', 'redcap_event_name', 'mri_date']]

    # Get old ID mapped to anon id from pre redcap project
    dfp = pd.DataFrame(dfp)
    dfp['ID'] = dfp[rc_pre.def_field].map(secondary_map(rc_pre))
    dfp = dfp[dfp.anon_id != '']
    dfp = dfp[['ID', 'anon_id']]

    # Get anon_id with anon date from anon redcap project
    dfa = pd.DataFrame(dfa)
    dfa['anon_id'] = dfa[rc_anon.def_field].map(secondary_map(rc_anon))
    dfa = dfa[dfa.mri_date != '']
    dfa = dfa[['anon_id', 'redcap_event_name', 'mri_date']]
    dfa = dfa.rename(columns={'mri_date': 'anon_date'})

    # Merge all together to get one row per mri with both ids and both dates
    df = pd.merge(dfp, dfd, on='ID')
    df = pd.merge(df, dfa, on=['anon_id', 'redcap_event_name'])
    df = df.sort_values('ID')

    return df


def anon_dicom(in_path, out_path, anon_subject, anon_session, anon_date):
    # Load DICOM
    d = pydicom.dcmread(in_path)

    # Delete UID from file metadata
    del d.file_meta[0x0002,0x0003]

    # Get just the date portion
    old_date = d.AcquisitionDateTime[:8]

    # Get just the time portion
    old_time =  d.AcquisitionDateTime[8:]

    # Get datetime formatted for DICOM, YYYYMMDDHHMMSS.000
    new_date = anon_date.replace('-', '')
    new_datetime =  new_date + old_time

    # Modify DICOM
    d.PatientID = anon_session
    d.PatientName = anon_subject
    d.StudyDescription = anon_session

    # Delete these fields completely
    for field in DELETE_FIELDS:
        if field in d:
            del d[field]

    # Replace these fields with date
    for field in DATE_FIELDS:
        if field in d:
            d[field].value = new_date

    # Replace these fields with datetime
    for field in DATETIME_FIELDS:
        if field in d:
            d[field].value = new_datetime

    d.remove_private_tags()

    # Delete UID tags from each frame
    g = '0x52009229'
    for frame in d[g]:
        if 0x00081140 in frame:
            for frame2 in frame[0x00081140]:
                del frame2[0x00081155]

    # Replace date/time in each frame
    g = '0x52009230'
    for frame in d[g]:
        for frame2 in frame['0x00209111']:
            frame2[0x00189074].value = new_datetime
            frame2[0x00189151].value = new_datetime

    # Save modified DICOM
    print(f'Saving:{out_path}')
    d.save_as(out_path)

    return d


def anonymize_scan(in_dir, out_dir, anon_subject, anon_session, anon_date):
    # anonymize each file
    for i, d in enumerate(sorted(os.listdir(in_dir))):
        in_dicom = f'{in_dir}/{d}'
        out_dicom = f'{out_dir}/{i}.dcm'
        os.makedirs(os.path.dirname(out_dicom), exist_ok=True)
        d = anon_dicom(in_dicom, out_dicom, anon_subject, anon_session, anon_date)


def anonymize_session(in_dir, out_dir, anon_subject, anon_session, anon_date):
    # Mirror scan folder names
    for scan in sorted(os.listdir(in_dir)):
        if scan.startswith('.'):
                continue

        scan_in_dir = f'{in_dir}/{scan}/DICOM'
        scan_out_dir = f'{out_dir}/{scan}/DICOM'
        anonymize_scan(
            scan_in_dir,
            scan_out_dir,
            anon_subject,
            anon_session,
            anon_date
        )


def get_session_date(session_dir):
    dicom_path = glob(f'{session_dir}/*/DICOM/*.dcm')[0]
    dicom_data = pydicom.dcmread(dicom_path)
    d = dicom_data.AcquisitionDateTime
    return '-'.join([d[:4], d[4:6], d[6:8]])


def get_session_suffix(subject, session):
    return session.split(subject)[1]


def anonymize_project(in_dir, out_dir, df):
    for subject in sorted(os.listdir(in_dir)):
        print(subject)
        if subject.startswith('.'):
                continue

        for session in sorted(os.listdir(f'{in_dir}/{subject}')):
            if session.startswith('.'):
                continue

            sess_in_dir = f'{in_dir}/{subject}/{session}'
            sess_date = get_session_date(sess_in_dir)

            # Locate the matching record to get anon id/date
            try:
                rec = df[(df['ID'] == subject) & (df['mri_date'] == sess_date)].iloc[0]
            except Exception as err:
                print(f'No match found for session:{subject}:{sess_date}:{err}')
                continue

            sess_suffix = get_session_suffix(subject, session)
            anon_subject = rec['anon_id']
            anon_session = f'{anon_subject}{session_suffix}'
            anon_date = f'{rec["anon_date"]}'
            sess_out_dir = f'{out_dir}/{anon_subject}/{anon_session}'
            anonymize_session(
                sess_in_dir,
                sess_out_dir,
                anon_subject,
                anon_session,
                anon_date
            )


def check_dicom(in_path, value):
    d = pydicom.dcmread(in_path)
    matches = _check_dicom(d, value)
    if matches:
        print(matches)

    return matches


def _check_dicom(dicom, value, matches=[]):
    for cur in dicom:
        if str(value) in str(cur.value):
            matches.append(f'{cur.keyword}:{cur.tag}:{cur.value}')

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
