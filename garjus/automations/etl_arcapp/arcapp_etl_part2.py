import tempfile
import os
import logging

import pandas as pd

from utils_redcap import get_redcap, download_file


# Load excel file into pandas dataframe
def parse_arc(filename):
    df = pd.DataFrame()

    try:
        # Load Data
        df = pd.read_excel(filename)
    except ValueError as err:
        logging.error('failed to read excel:{}:{}'.format(filename, err))

    return df


# Load ARC excel file and calculate summary measures
def extract_arc(filename):
    data = {}

    try:
        # Load excel file into dataframe
        df = parse_arc(filename)

        if len(df) > 29 or len(df) < 3:
            msg = 'extract failed, wrong number of rows:{}'.format(filename)
            logging.error(msg)
            return None

        # Get elapsed time of testing sessions
        df['startTime'] = pd.to_datetime(df['startTime'], errors='coerce')
        df['completeTime'] = pd.to_datetime(df['completeTime'], errors='coerce')
        df['arctime'] = (df.completeTime - df.startTime) / pd.Timedelta(seconds=1)

        # First, calculate for whole week

        # Count complete sessions
        data['warcsesscomp'] = len(df[df.finishedSession == True])
        if data['warcsesscomp'] > 0:
            data['arcweekany'] = 1
        else:
            data['arcweekany'] = 0

        # Calculate means
        data['wmeanarctime'] = df.arctime.mean()
        data['wmeansymbolsrt'] = df.symbolsRT.mean()
        data['wmeansymbolsacc'] = df.symbolsAcc.mean()
        data['wmeanpricesrt'] = df.pricesRT.mean()
        data['wmeangrided'] = df.gridEd.mean()

        # Calculate SD
        data['wsdarctime'] = df.arctime.std()
        data['wsdsymbolsrt'] = df.symbolsRT.std()
        data['wsdsymbolsacc'] = df.symbolsAcc.std()
        data['wsdpricesrt'] = df.pricesRT.std()
        data['wsdgrided'] = df.gridEd.std()

        # Calculate CV
        data[f'wcovsymbolsrt'] = df.symbolsRT.std() / df.symbolsRT.mean()
        data[f'wcovsymbolsacc'] = df.symbolsAcc.std() / df.symbolsAcc.mean()
        data[f'wcovpricesrt'] = df.pricesRT.std() / df.pricesRT.mean()
        data[f'wcovgrided'] = df.gridEd.std() / df.gridEd.mean()

        # Loop days 1-7, calculate for each day
        for d in range(1,8):
            dfd = df[df['dayIndex'] == d]

            # Count complete sessions
            data[f'd{d}arcsesscomp'] = len(dfd[dfd.finishedSession == True])

            # Calculate means
            data[f'd{d}meanarctime'] = dfd.arctime.mean()
            data[f'd{d}meansymbolsrt'] = dfd.symbolsRT.mean()
            data[f'd{d}meansymbolsacc'] = dfd.symbolsAcc.mean()
            data[f'd{d}meanpricesrt'] = dfd.pricesRT.mean()
            data[f'd{d}meangrided'] = dfd.gridEd.mean()

            # Calculate SD
            data[f'd{d}sdarctime'] = dfd.arctime.std()
            data[f'd{d}sdsymbolsrt'] = dfd.symbolsRT.std()
            data[f'd{d}sdsymbolsacc'] = dfd.symbolsAcc.std()
            data[f'd{d}sdpricesrt'] = dfd.pricesRT.std()
            data[f'd{d}sdgrided'] = dfd.gridEd.std()

            # Calculate CV
            data[f'd{d}covsymbolsrt'] = dfd.symbolsRT.std() / dfd.symbolsRT.mean()
            data[f'd{d}covsymbolsacc'] = dfd.symbolsAcc.std() / dfd.symbolsAcc.mean()
            data[f'd{d}covpricesrt'] = dfd.pricesRT.std() / dfd.pricesRT.mean()
            data[f'd{d}covgrided'] = dfd.gridEd.std() / dfd.gridEd.mean()
            #from scipy.stats import variation 
            #variation(arr)
    except KeyError as err:
        msg = 'extract failed:{}:{}'.format(filename, err)
        logging.error(msg)
        return None

    # Return etl data
    return data


# Extract ARC excel from REDCap, transform to summary measures, load to REDCap
def etl_arc(project, record_id, event_id, tab_field):
    # Download the tab file from redcap to tmp
    tmpdir = tempfile.mkdtemp()
    basename = '{}-{}-{}.txt'.format(record_id, event_id, tab_field)
    tab_file = os.path.join(tmpdir, basename)
    result = download_file(project, record_id, event_id, tab_field, tab_file)
    if not result:
        logging.error('{}:{}:{}'.format(record_id, event_id, 'download failed'))
        return

    # Extract the data
    logging.info('{}:{}'.format('extracting', tab_file))
    arc_data = extract_arc(tab_file)
    if arc_data is None:
        logging.error('extract failed')
        return

    # Transform the data
    data = {
        project.def_field: record_id,
        'redcap_event_name': event_id,
    }
    for k, v in arc_data.items():
        data[f'arcapp_{k}'] = str(v)

    # Load the data back to redcap
    try:
        response = project.import_records([data])
        assert 'count' in response
        logging.info('{}:{}'.format('arcapp etl uploaded', record_id))                        
    except AssertionError as e:
        msg = 'arcapp_etl upload failed:{}:{}'.format(record_id, e)
        logging.error(msg)
        return


def process_project(
    project,
    events=['baselinemonth_0_arm_2','baselinemonth_0_arm_3'],
    tab_field='arcapp_csvfile',
    done_field='arcapp_d1arcsesscomp',
    use_secondary=True,
):
    results = []
    def_field = project.def_field
    fields = [def_field, tab_field, done_field]
    id2subj = {}

    if use_secondary:
        # Handle secondary ID
        sec_field = project.export_project_info()['secondary_unique_field']
        if not sec_field:
            logging.error('secondary enabled, but no secondary field found')
            return

        rec = project.export_records(fields=[def_field, sec_field])
        id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

    # Get records
    rec = project.export_records(fields=fields, events=events)

    # Process each record
    for r in rec:
        record_id = r[def_field]
        event = r['redcap_event_name']
        if use_secondary:
            try:
                subj = id2subj[record_id]
            except KeyError as err:
                logging.debug('record without subject number:{err}')
                continue
        else:
            subj = record_id

        # Make visit name for logging
        visit = '{}:{}'.format(subj, event)

        # Check for converted file
        if not r[tab_field]:
            logging.debug(visit + ':not yet converted')
            continue

        # Check for convert failed flag
        if r[tab_field] == 'CONVERT_FAILED.txt':
            logging.debug('{}:{}:{}'.format(record_id, event, 'CONVERT_FAILED'))
            continue

        # Check for missing data flag
        if r[tab_field] == 'MISSING_DATA.txt':
            logging.debug('{}:{}:{}'.format(record_id, event, 'MISSING_DATA'))
            continue

        # Determine if ETL has already been run
        if r[done_field]:
            logging.debug(visit + ':already ETL')
            continue

        # Do the ETL
        etl_arc(project, record_id, event, tab_field)
        logging.debug(visit + ':uploaded')
        results.append({
            'result': 'COMPLETE',
            'type': 'ETL',
            'subject': subj,
            'event': event,
            'field': tab_field})

    return results


if __name__ == "__main__":
# For development/testing we can create a connection to the test redcap 
# and run it. In production, this will be run by run_updates.py

    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.info('connecting to redcap')
    rc = get_redcap('167000')

    process_project(rc)

    logging.info('Done!')
