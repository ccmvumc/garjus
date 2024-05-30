import logging

import pandas as pd

from ...utils_redcap import get_redcap


#def _load(project, data):
#    # Load the data back to redcap
#    try:
#        _response = project.import_records(data)
#        assert 'count' in _response
#        return True
#    except (AssertionError, Exception) as err:
#        logger.error(err)
#        return False


# Load ARC tests and calculate summary measures
def extract_arc(records):
    data = {}

    try:
        # Load into dataframe
        df = pd.DataFrame(records)
        print(df)

        if len(df) > 29 or len(df) < 3:
            msg = 'extract failed, wrong number of rows:{}'.format(filename)
            logging.error(msg)
            return None

        # First, calculate for whole week

        # Count complete sessions
        data['warcsesscomp'] = len(df[df.finishedSession == True])
        if data['warcsesscomp'] > 0:
            data['arcweekany'] = 1
        else:
            data['arcweekany'] = 0

        # Calculate means
        data['wmeansymbolsrt'] = df.symbolsRT.mean()
        data['wmeansymbolsacc'] = df.symbolsAcc.mean()
        data['wmeanpricesrt'] = df.pricesRT.mean()
        data['wmeangrided'] = df.gridEd.mean()

        # Calculate SD
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
            data[f'd{d}meansymbolsrt'] = dfd.symbolsRT.mean()
            data[f'd{d}meansymbolsacc'] = dfd.symbolsAcc.mean()
            data[f'd{d}meanpricesrt'] = dfd.pricesRT.mean()
            data[f'd{d}meangrided'] = dfd.gridEd.mean()

            # Calculate SD
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


def process_project(project):
    results = []
    def_field = project.def_field
    fields = [def_field, 'arc_finishedsession', 'arc_response_date']
    subj2id = {}
    subjects = []
    #'arcdaily_d1arcsesscomp',

    # Handle secondary ID
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        subj2id = {x[sec_field]: x[def_field] for x in rec if x[sec_field]}
        subjects = sorted(list(set([x[sec_field] for x in rec if x[sec_field]])))
    else:
        rec = project.export_records(fields=[def_field])
        subj2id = {x[def_field]: x[def_field] for x in rec if x[def_field]}
        subjects = sorted(list(set([x[def_field] for x in rec])))

    print(subjects)

    # Get records
    all_records = project.export_records(fields=fields)

    # Get the arcdata repeating records
    arc_records = [x for x in all_records if x['redcap_repeat_instance']]

    # Process each subject
    for subj in subjects:
        subj_id = subj2id[subj]
        subj_events = list(set([x['redcap_event_name'] for x in all_records if x[def_field] == subj_id]))
        subj_arc = [x for x in arc_records if x[def_field] == subj_id]

        # Iterate subject events
        for event_id in subj_events:

            # Find existing numcomplete
            #numcomplete = [x for x in all_records if (x[def_field] == subj_id) and (x['redcap_event_name'] == event_id) and (x.get('arcapp_numcomplete', False))]
            #if len(numcomplete) > 0:
            #    # numcomplete already set
            #    continue

            # Get repeat records
            repeats = [x for x in subj_arc if (x['redcap_event_name'] == event_id) and (x.get('arc_response_date', False))]
            if len(repeats) == 0:
                # no repeats to count
                continue

            # Check date of tests, if less than week since starting, skip
            first_date = sorted([x['arc_response_date'] for x in repeats])[0]
            if (datetime.today() - datetime.strptime(first_date, '%Y-%m-%d')).days <  7:
                logger.debug(f'SKIP:{subj}:{event_id}:{first_date}')
                continue

            finished = [x for x in subj_arc if (x['redcap_event_name'] == event_id) and (x.get('arc_finishedsession', False) == '1')]
            count_finished = len(finished)

            logger.debug(f'{subj}:{event_id}:{first_date}:{count_finished=}')

            # set numcomplete equal to count_finished for record/event
            #data = {
            #    def_field: subj_id,
            #    'redcap_event_name': event_id,
            #    'arcapp_numcomplete': str(count_finished),
            #    'arc_app_complete': '2',
            #}
            #logger.debug(f'loading numcomplete:{subj_id}:{event_id}')

            #_load(project, [data])

            #results.append({
            #    'result': 'COMPLETE',
            #    'description': 'arc_summary',
            #    'category': 'arc_summary',
            #    'subject': subj_id,
            #    'event': event_id,
            #    'field': 'arcapp_numcomplete'})
            print(repeats)
            data = extract_arc(repeats)
            print(data)

    return results

