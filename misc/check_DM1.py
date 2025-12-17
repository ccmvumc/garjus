import sys

from garjus.anonymize import load_link, check_project
from garjus.utils_redcap import get_redcap


if __name__ == '__main__':
    # Get top-level directory from command-line
    root_out_dir = sys.argv[1]

    # Get table of old id, new id, old date, new date
    print('Loading link from REDCap')
    rc_pre = get_redcap('222586')
    rc_anon = get_redcap('222631')
    link_data = load_link(rc_pre, rc_anon)

    print('Checking anonymization to DICOM')
    check_project(root_out_dir, link_data)
