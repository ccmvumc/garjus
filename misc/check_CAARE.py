import sys

import garjus

from .check import check_project


if __name__ == '__main__':
    # Get top-level directory from command-line
    root_out_dir = sys.argv[1]

    # Get table of old id, new id, old date, new date
    print('Loading link from REDCap')
    rc_pre = garjus.utils_redcap.get_redcap('220310')
    rc_anon = garjus.utils_redcap.get_redcap('222564')
    link_data = load_link(rc_pre, rc_anon)

    print('Checking anonymization to DICOM')
    check_project(root_out_dir, link_data)
