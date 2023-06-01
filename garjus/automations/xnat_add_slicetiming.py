import os
import tempfile
import json
import logging


def process_project(
    garjus,
    project,
    slicetiming,
    scantypes,
    sites=None
):
    # Get the scans, filter to only fmri_rest1,fmri_rest that start with 14*
    # download the JSON, upload to JSON_NoSliceTiming, add SliceTiming, upload
    # slicetiming is an list of floats
    results = []

    logging.debug(f'xnat_add_slicetiming:loading data:{project}')
    df = garjus.scans(projects=[project])

    # Filter
    df = df[df['SCANTYPE'].isin(scantypes)]
    if sites:
        df = df[df['SITE'].isin(sites)]

    # Check each scan
    for i, scan in df.iterrows():
        if 'JSON_MissingSliceTiming' in scan['RESOURCES']:
            continue

        full_path = scan['full_path']
        logging.debug(f'adding slicetiming:{full_path}')

        res = garjus.xnat().select(f'{full_path}/resources/JSON')

        files = res.files().get()
        if len(files) == 0:
            logging.debug(f'no JSON files found:{full_path}')
            continue
        elif len(files) > 1:
            logging.debug(f'too many JSON files found:{full_path}')
            continue

        src = files[0]

        with tempfile.TemporaryDirectory() as tmpdir:
            dst = os.path.join(tmpdir, src)
            res.file(src).get(dst)

            if has_slicetiming(dst):
                logging.debug(f'already has SliceTiming:{full_path}')
                continue

            # upload to no slicetiming
            new_res = res.parent().resource('JSON_MissingSliceTiming')
            if new_res.exists():
                logging.debug(f'JSON_MissingSliceTiming already exists:{full_path}')
            else:
                logging.debug(f'saving to JSON_MissingSliceTiming:{full_path}')
                new_res.file(src).put(dst)

            # add it and upload
            add_slicetiming(dst, slicetiming)
            logging.debug(f'uploading to JSON:{full_path}:{dst}')
            res.file(src).put(dst, overwrite=True)

            results.append({
                'result': 'COMPLETE',
                'description': 'add slicetiming',
                'subject': scan['SUBJECT'],
                'session': scan['SESSION'],
                'scan': scan['SCANID']})

    return results


def add_slicetiming(jsonfile, slicetiming):
    with open(jsonfile, 'r+') as f:
        data = json.load(f)
        data['SliceTiming'] = slicetiming
        f.seek(0)
        json.dump(data, f, indent=4)


def has_slicetiming(jsonfile):
    with open(jsonfile, 'r') as f:
        return ('SliceTiming' in f.read())


