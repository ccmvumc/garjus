import sys

import garjus

from .anonymize import anonymize_project


if __name__ == '__main__':
    # Get top-level directory from command-line
    root_in_dir = sys.argv[1]
    root_out_dir = sys.argv[2]

    # Get table of old id, new id, old date, new date
    print('Loading link from REDCap')
    rc_pre = garjus.utils_redcap.get_redcap('222462')
    rc_anon = garjus.utils_redcap.get_redcap('222566')
    df = load_link(rc_pre, rc_anon)

    print('Applying anonymization to DICOM')
    anonymize_project(
        root_in_dir,
        root_out_dir,
        df
    )
