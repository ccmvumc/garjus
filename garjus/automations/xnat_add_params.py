import os
import tempfile
import json
import logging


def process_project(
    garjus,
    project,
    params,
    scantypes,
    sites=None
):
    # Get scans, filter, download JSON, 
    # upload to JSON_BeforeParams,
    # add params, upload to JSON
    results = []

    logging.debug(f'loading data:{project}')
    df = garjus.scans(projects=[project])

    # Filter
    df = df[df['SCANTYPE'].isin(scantypes)]
    if sites:
        df = df[df['SITE'].isin(sites)]

    # Check each scan
    for i, scan in df.iterrows():
        if 'JSON_BeforeParams' in scan['RESOURCES']:
            logging.debug(f'JSON_BeforeParams exists:{i}:{scan["full_path"]}')
            continue

        full_path = scan['full_path']
        logging.debug(f'adding params:{full_path}')

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

            if has_params(dst):
                logging.debug(f'already has params:{full_path}')
                continue

            # save a copy of unaltered
            new_res = res.parent().resource('JSON_BeforeParams')
            if new_res.exists():
                logging.debug(f'JSON_BeforeParams exists:{full_path}')
            else:
                logging.debug(f'saving to JSON_BeforeParams:{full_path}')
                new_res.file(src).put(dst)

            # add it and upload
            add_params(dst, params)
            logging.debug(f'uploading to JSON:{full_path}:{dst}')
            res.file(src).put(dst, overwrite=True)

            results.append({
                'result': 'COMPLETE',
                'description': 'add params',
                'subject': scan['SUBJECT'],
                'session': scan['SESSION'],
                'scan': scan['SCANID']})

    return results


def has_params(jsonfile):
    with open(jsonfile, 'r') as f:
        return ('RepetitionTimePreparation' in f.read())


def add_params(jsonfile, params):
    with open(jsonfile, 'r+') as f:
        data = json.load(f)
        data.update(params)
        f.seek(0)
        json.dump(data, f, indent=4)
