"""

update will create any missing

"""
from datetime import datetime
import tempfile
import csv
import logging
import glob
import os


def update(garjus, projects):
    """Update project progress."""
    for p in projects:
        proctypes = garjus.proctypes(p)
        if not proctypes:
            proctypes = garjus.default_proctypes()
        update_project(garjus, p, proctypes)


def update_project(garjus, project, proctypes):
    """Update project proctypes."""
    logging.debug(f'stats updating project:{project},proctypes={proctypes}')
    # Get list of all assessors
    dfa = garjus.assessors([project], proctypes)
    logging.debug(f'total assessors:{len(dfa)}')

    # Get list of assessors already in stats
    existing = garjus.stats_assessors(project, proctypes)

    # Filter to remove already uploaded
    dfa = dfa[~dfa['ASSR'].isin(existing)]
    logging.debug(f'assessors after filtering out already uploaded:{len(dfa)}')

    # Filter to only COMPLETE
    dfa = dfa[dfa['PROCSTATUS'] == 'COMPLETE']
    logging.debug(f'assessors after filtering only COMPLETE:{len(dfa)}')

    # Filter to not Failed
    dfa = dfa[dfa['QCSTATUS'] != 'Failed']
    logging.debug(f'assessors after filtering out QC Failed:{len(dfa)}')

    # Iterate xnat assessors
    for r in dfa.sort_values('ASSR').to_dict('records'):
        try:
            update_assessor(
                garjus,
                r['PROJECT'],
                r['SUBJECT'],
                r['SESSION'],
                r['ASSR'],
            )
        except ConnectionError as err:
            logging.info(err)
            logging.info('waiting a minute')
            os.sleep(60)

    # TODO: Delete assessors no longer needed
    #delete_list = list(set().difference())
    #for assr in delete_list:
    #    # delete(assr)
    #    logging.info(f'TBD:deleting:{assr}')


def update_assessor(garjus, proj, subj, sess, assr):
    """Update assessor stats."""
    logging.info(f'uploading assessor stats:{assr}')
    with tempfile.TemporaryDirectory() as tmpdir:
        _dir = garjus.get_source_stats(proj, subj, sess, assr, tmpdir)
        _stats = transform_stats(_dir)
        garjus.set_stats(proj, subj, sess, assr, _stats)


def transform_stats(stats_dir):
    """Transform stats from directory of files to dict."""
    data = {}

    if os.path.exists(f'{stats_dir}/stats.csv'):
        data = _load_stats(f'{stats_dir}/stats.csv')
    elif os.path.exists(f'{stats_dir}/stats.txt'):
        data = _load_stats(f'{stats_dir}/stats.txt')
    elif len(glob.glob(f'{stats_dir}/*stats.txt')) > 0:
        for txt_path in glob.iglob(f'{stats_dir}/*stats.txt'):
            data.update(_load_stats_tall(txt_path))
    else:
        # Handle proctypes that output multiple csv
        for csv_path in glob.iglob(f'{stats_dir}/*.csv'):
            data.update(_load_stats_wide(csv_path))

    return data


def _isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


def _load_stats_wide(filename):
    data = {}

    with open(filename, newline='') as f:
        # Connect csv reader
        reader = csv.reader(f)

        # Load header from first line
        header = next(reader)

        # Read data from subsequent lines
        for line in reader:
            for i, v in enumerate(line):
                data[header[i]] = v

    return data


def _load_stats_tall(filename):
    data = {}
    rows = []
    with open(filename) as f:
        rows = f.readlines()

    for r in rows:
        (k, v) = r.strip().replace('=', ',').split(',')
        data[k] = v

    return data


def _load_stats(filename):
    lines = []
    with open(filename) as f:
        lines = f.readlines()

    if ('=' in lines[0]) or (len(lines) >= 3 and len(lines[0].split(',')) <= 3):
        return _load_stats_tall(filename)
    else:
        return _load_stats_wide(filename)
