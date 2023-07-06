"""Downlad stats upload to vol_txt in XNAT."""
import logging
import tempfile
import csv


logger = logging.getLogger('garjus.automations.xnat_ma3stats2voltxt')


def process_project(
    xnat,
    project,
    assessors,
):
    """Process project."""
    results = []

    for i, assr in assessors.iterrows():
        vol_res = xnat.select_assessor_resource(
            assr['PROJECT'],
            assr['SUBJECT'],
            assr['SESSION'],
            assr['ASSR'],
            'VOL_TXT')

        if vol_res.exists():
            logger.debug('VOL_TXT exists')
            continue

        stats_res = xnat.select_assessor_resource(
            assr['PROJECT'],
            assr['SUBJECT'],
            assr['SESSION'],
            assr['ASSR'],
            'STATS')

        if not stats_res.exists():
            logger.debug('STATS does not exist')
            continue

        # Download stats, transform, upload
        with tempfile.TemporaryDirectory() as tmpdir:
            vol_file = f'{tmpdir}/target_processed_label_volumes.txt'
            stats_file = f'{tmpdir}/stats.csv'

            # Extract stats file
            logger.debug('download stats')
            xnat.select_assessor_resource(
                assr['PROJECT'],
                assr['SUBJECT'],
                assr['SESSION'],
                assr['ASSR'],
                'STATS'
            ).file('stats.csv').get(stats_file)

            # Transform it
            stats2voltxt(stats_file, vol_file)

            # Upload it
            logger.debug('upload voltxt')
            xnat.select_assessor_resource(
                assr['PROJECT'],
                assr['SUBJECT'],
                assr['SESSION'],
                assr['ASSR'],
                'VOL_TXT'
            ).file('target_processed_label_volumes.txt').put(vol_file)

        results.append({
            'result': 'COMPLETE',
            'description': 'ma3stats2voltxt',
            'subject': assr['SUBJECT'],
            'session': assr['SESSION'],
            'assessor': assr['ASSR']})

    return results


def stats2voltxt(stats, voltxt):
    with open(stats, newline='') as f:
        reader = csv.reader(f)
        keys = next(reader)
        values = next(reader)

    # Remove first two items that are not found in MA v2
    keys = keys[2:]
    values = values[2:]

    with open(voltxt, 'w') as f:
        f.write('Name,Name,Volume\n')
        for i, k in enumerate(keys):
            f.writelines(f'{k},{k},{values[i]}\n')
