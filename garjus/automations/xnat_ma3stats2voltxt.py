"""dcm2niix scans in XNAT."""
import logging
import subprocess as sb
import tempfile
import os
import pathlib
import glob

from .. import utils_dcm2nii
from .. import utils_xnat


logger = logging.getLogger('garjus.automations.xnat_ma3stats2voltxt')


def process_project(
    xnat,
    project,
    assessors,
):
    """Process project."""
    results = []

    for i, assr in assessors.iterrows():

        if 'VOLTXT' in assr['RESOURCES']:
            logger.debug(f'{i}:{assr.ASSR}:VOLTXT exists')
            continue

        if 'STATS' not in assr['RESOURCES']:
            logger.debug(f'{i}:{assr.ASSR}:stats does not exist')
            continue

        # Download stats, transform, upload
        with tempfile.TemporaryDirectory() as tmpdir:
            vol_file = f'tmpdir/target_processed_label_volumes.txt'
            stats_file = f'{tmpdir}/stats.csv'

            # Extract stats file
            print('download stats')
            xnat.select_assessor_resource(
                assr['project_label'],
                assr['subject_label'],
                assr['session_label'],
                assr['assessor_label'],
                'STATS'
            ).file('stats.csv').get(stats_file)

            # Transform it
            print('transform')
            stats2voltxt(stats_file, vol_file)

            # Upload it
            print('upload voltxt')
            xnat.select_assessor_resource(
                assr['project_label'],
                assr['subject_label'],
                assr['session_label'],
                assr['assessor_label'],
                'VOL_TXT'
            ).file('target_processed_label_volumes.txt').put(vol_file)

        results.append({
            'result': 'COMPLETE',
            'description': 'ma3stats2voltxt',
            'subject': assr.SUBJECT,
            'session': assr.SESSION,
            'assessor': assr.ASSR})

    return results


def stats2voltxt(stats, voltxt):
    with open(stats, newline='') as f:
      reader = csv.reader(f)
      keys = next(reader)
      values = next(reader)

    # Remove first two items not found in MA v2
    keys = keys[2:]
    values = values[2:]

    with open(voltxt, 'w') as f:
        f.write('Name,Name,Volume\n')
        for i, k in enumerate(keys):
            f.writelines(f'{k},{k},{values[i]}\n')
