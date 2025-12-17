import sys

from garjus.anonymize import anonymize_project, load_link
from garjus.utils_redcap import get_redcap


if __name__ == '__main__':
    # Get top-level directory from command-line
    root_in_dir = sys.argv[1]
    root_out_dir = sys.argv[2]

    # Get table of old id, new id, old date, new date
    print('Loading link from REDCap')
    rc_pre = get_redcap('222586')
    rc_anon = get_redcap('222631')
    df = load_link(rc_pre, rc_anon)

    print('Applying anonymization to DICOM')
    anonymize_project(
        root_in_dir,
        root_out_dir,
        df
    )
