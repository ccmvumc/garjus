import logging
from datetime import datetime

import pandas as pd


logger = logging.getLogger('garjus.automations.etl_arcapp.dailyweekly')


def _load(project, data):
    # Load the data back to redcap
    try:
        _response = project.import_records(data)
        assert 'count' in _response
        return True
    except (AssertionError, Exception) as err:
        logger.error(err)
        return False


# Load ARC tests and calculate summary measures
def extract_arc(records):
    data = {}

    # Load into dataframe
    df = pd.DataFrame(records)

    # Count complete sessions
    data['arc_warcsesscomp'] = len(df[df['arc_finishedsession'] == '1'])

    # Calculate range for whole week
    data[f'arc_wrangesymbolsrt'] = \
        df.arc_symbolsrt.astype(float).max() - \
        df.arc_symbolsrt.astype(float).min()
    data[f'arc_wrangesymbolsacc'] = \
        df.arc_symbols_accuracy.astype(float).max() - \
        df.arc_symbols_accuracy.astype(float).min()
    data[f'arc_wrangepricesrt'] = \
        df.arc_pricesrt.astype(float).max() - \
        df.arc_pricesrt.astype(float).min()
    data[f'arc_wrangepricesacc'] = \
        df.arc_prices_accuracy.astype(float).max() - \
        df.arc_prices_accuracy.astype(float).min()
    data[f'arc_wrangegrided'] = \
        df.arc_grided.astype(float).max() - \
        df.arc_grided.astype(float).min()

    # Calculate means for whole week
    data['arc_wmeansymbolsrt'] = df.arc_symbolsrt.astype(float).mean()
    data['arc_wmeansymbolsacc'] = df.arc_symbols_accuracy.astype(float).mean()
    data['arc_wmeanpricesrt'] = df.arc_pricesrt.astype(float).mean()
    data['arc_wmeanpricesacc'] = df.arc_prices_accuracy.astype(float).mean()
    data['arc_wmeangrided'] = df.arc_grided.astype(float).mean()

    # Calculate SD for whole week
    data['arc_wsdsymbolsrt'] = df.arc_symbolsrt.astype(float).std()
    data['arc_wsdsymbolsacc'] = df.arc_symbols_accuracy.astype(float).std()
    data['arc_wsdpricesrt'] = df.arc_pricesrt.astype(float).std()
    data['arc_wsdpricesacc'] = df.arc_prices_accuracy.astype(float).std()
    data['arc_wsdgrided'] = df.arc_grided.astype(float).std()

    # Calculate CV for whole week
    data[f'arc_wcovsymbolsrt'] = \
        df.arc_symbolsrt.astype(float).std() / \
        df.arc_symbolsrt.astype(float).mean()
    data[f'arc_wcovsymbolsacc'] = \
        df.arc_symbols_accuracy.astype(float).std() / \
        df.arc_symbols_accuracy.astype(float).mean()
    data[f'arc_wcovpricesrt'] = \
        df.arc_pricesrt.astype(float).std() / \
        df.arc_pricesrt.astype(float).mean()
    data[f'arc_wcovpricesacc'] = \
        df.arc_prices_accuracy.astype(float).std() / \
        df.arc_prices_accuracy.astype(float).mean()
    data[f'arc_wcovgrided'] = \
        df.arc_grided.astype(float).std() / \
        df.arc_grided.astype(float).mean()

    # Loop days 1-7, calculate for each day
    for d in range(1, 8):
        dfd = df[df['arc_day_index'] == str(d)]

        # Count complete sessions
        data[f'arc_d{d}arcsesscomp'] = \
            len(dfd[dfd['arc_finishedsession'] == '1'])

        # Calculate ranges
        data[f'arc_d{d}rangesymbolsrt'] = \
            dfd.arc_symbolsrt.astype(float).max() - \
            dfd.arc_symbolsrt.astype(float).min()
        data[f'arc_d{d}rangesymbolsacc'] = \
            dfd.arc_symbols_accuracy.astype(float).max() - \
            dfd.arc_symbols_accuracy.astype(float).min()
        data[f'arc_d{d}rangepricesrt'] = \
            dfd.arc_pricesrt.astype(float).max() - \
            dfd.arc_pricesrt.astype(float).min()
        data[f'arc_d{d}rangepricesacc'] = \
            dfd.arc_prices_accuracy.astype(float).max() - \
            dfd.arc_prices_accuracy.astype(float).min()
        data[f'arc_d{d}rangegrided'] = \
            dfd.arc_grided.astype(float).max() - \
            dfd.arc_grided.astype(float).min()

        # Calculate means
        data[f'arc_d{d}meansymbolsrt'] = dfd.arc_symbolsrt.astype(float).mean()
        data[f'arc_d{d}meansymbolsacc'] = \
            dfd.arc_symbols_accuracy.astype(float).mean()
        data[f'arc_d{d}meanpricesrt'] = dfd.arc_pricesrt.astype(float).mean()
        data[f'arc_d{d}meanpricesacc'] = \
            dfd.arc_prices_accuracy.astype(float).mean()
        data[f'arc_d{d}meangrided'] = dfd.arc_grided.astype(float).mean()

        # Calculate SD
        data[f'arc_d{d}sdsymbolsrt'] = dfd.arc_symbolsrt.astype(float).std()
        data[f'arc_d{d}sdsymbolsacc'] = \
            dfd.arc_symbols_accuracy.astype(float).std()
        data[f'arc_d{d}sdpricesrt'] = dfd.arc_pricesrt.astype(float).std()
        data[f'arc_d{d}sdpricesacc'] = \
            dfd.arc_prices_accuracy.astype(float).std()
        data[f'arc_d{d}sdgrided'] = dfd.arc_grided.astype(float).std()

        # Calculate CV
        data[f'arc_d{d}covsymbolsrt'] = \
            dfd.arc_symbolsrt.astype(float).std() / \
            dfd.arc_symbolsrt.astype(float).mean()
        data[f'arc_d{d}covsymbolsacc'] = \
            dfd.arc_symbols_accuracy.astype(float).std() / \
            dfd.arc_symbols_accuracy.astype(float).mean()
        data[f'arc_d{d}covpricesrt'] = \
            dfd.arc_pricesrt.astype(float).std() / \
            dfd.arc_pricesrt.astype(float).mean()
        data[f'arc_d{d}covpricesacc'] = \
            dfd.arc_prices_accuracy.astype(float).std() / \
            dfd.arc_prices_accuracy.astype(float).mean()
        data[f'arc_d{d}covgrided'] = \
            dfd.arc_grided.astype(float).std() / \
            dfd.arc_grided.astype(float).mean()

    # Return etl data
    return data


def process_project(project):
    results = []
    def_field = project.def_field
    fields = [
        def_field,
        'arc_finishedsession',
        'arc_response_date',
        'arc_warcsesscomp'
    ]

    subj2id = {}
    subjects = []

    # Handle secondary ID
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        subj2id = {x[sec_field]: x[def_field] for x in rec if x[sec_field]}
        subjects = list(set([x[sec_field] for x in rec if x[sec_field]]))
        subjects = sorted(subjects)
    else:
        rec = project.export_records(fields=[def_field])
        subj2id = {x[def_field]: x[def_field] for x in rec if x[def_field]}
        subjects = sorted(list(set([x[def_field] for x in rec])))

    print(subjects)
    subjects = ['14063']

    # Get records
    all_records = project.export_records(fields=fields, forms=['arc_data'])

    # Get the arcdata repeating records
    arc_records = [x for x in all_records if x['redcap_repeat_instance'] and x['redcap_repeat_instrument'] == 'arc_data']

    # Process each subject
    for subj in subjects:
        subj_id = subj2id[subj]
        subj_events = sorted(list(set([x['redcap_event_name'] for x in all_records if x[def_field] == subj_id])))
        subj_arc = [x for x in arc_records if x[def_field] == subj_id]

        print(subj_events)

        # Iterate subject events
        for event_id in subj_events:

            # Find existing
            found = [x for x in all_records if (x[def_field] == subj_id) and (x['redcap_event_name'] == event_id) and (x.get('arc_warcsesscomp', False))]
            if len(found) > 0:
                logger.debug(f'found existing:{subj}:{event_id}')
                continue

            # Get repeat records
            repeats = [x for x in subj_arc if (x['redcap_event_name'] == event_id)]
            if len(repeats) == 0:
                # no repeats to count
                logger.debug(f'no repeats found:{subj}:{event_id}')
                continue

            # Check date of tests, if less than week since starting, skip
            dates = [x['arc_response_date'] for x in repeats if x['arc_response_date']]
            if len(dates) > 0:
                first_date = sorted(dates)[0]
                if (datetime.today() - datetime.strptime(first_date, '%Y-%m-%d')).days < 7:
                    logger.debug(f'SKIP:{subj}:{event_id}:{first_date}')
                    continue

            finished = [x for x in subj_arc if (x['redcap_event_name'] == event_id) and (x.get('arc_finishedsession', '0') == '1')]
            count_finished = len(finished)

            logger.debug(f'{subj}:{event_id}:{count_finished=}')

            data = {
                def_field: subj_id,
                'redcap_event_name': event_id,
                'arc_dailyweekly_complete': '2',
            }

            if count_finished > 0:
                # Calculate daily and weekly
                data.update(extract_arc(finished))
            else:
                logger.debug(f'no finished records found:{subj}:{event_id}')
                data.update({'arc_warcsesscomp': '0'})

            result = _load(project, [data])
            if result is True:
                results.append({
                    'result': 'COMPLETE',
                    'description': 'arc_dailyweekly',
                    'category': 'arc_dailyweekly',
                    'subject': subj_id,
                    'event': event_id
                })

    return results
