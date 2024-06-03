import logging
from datetime import datetime

import pandas as pd


logger = logging.getLogger('garjus.automations.etl_lifedata.dailyweekly')


def _load(project, data):
    # Load the data back to redcap
    try:
        _response = project.import_records(data)
        assert 'count' in _response
        return True
    except (AssertionError, Exception) as err:
        logger.error(err)
        return False


# Load Life tests and calculate summary measures
def extract_life(records):
    data = {}

    # Load into dataframe
    df = pd.DataFrame(records)

    # Set day using first real notification as baseline, skip 0th
    df['lifedata_notification_time'] = pd.to_datetime(
        df['lifedata_notification_time'], errors='coerce')
    df = df.sort_values('lifedata_notification_time')
    if len(df) > 1:
        df['day'] = (df['lifedata_notification_time'] - df.iloc[1]['lifedata_notification_time']).dt.days
    else:
        df['day'] = (df['lifedata_notification_time'] - df.iloc[0]['lifedata_notification_time']).dt.days

    df['day'] = df['day'] + 1

    # Count complete sessions for the week
    data['life_wlifesesscomp'] = len(df[df['lifedata_responded'] == '1'])

    # Convert format to float so we can math
    df['lifedata_deptot'] = pd.to_numeric(
        df['lifedata_deptot'], errors='coerce')
    df['lifedata_fatiguetot'] = pd.to_numeric(
        df['lifedata_fatiguetot'], errors='coerce')
    df['lifedata_negtot'] = pd.to_numeric(
        df['lifedata_negtot'], errors='coerce')
    df['lifedata_postot'] = pd.to_numeric(
        df['lifedata_postot'], errors='coerce')
    df['lifedata_rumtot'] = pd.to_numeric(
        df['lifedata_rumtot'], errors='coerce')
    df['lifedata_stress'] = pd.to_numeric(
        df['lifedata_stress'], errors='coerce')

    # Calculate range for whole week
    data[f'life_wrange_deptot'] = \
        df.lifedata_deptot.max() - df.lifedata_deptot.min()
    data[f'life_wrange_fatiguetot'] = \
        df.lifedata_fatiguetot.max() - df.lifedata_fatiguetot.min()
    data[f'life_wrange_pnnegtot'] = \
        df.lifedata_negtot.max() - df.lifedata_negtot.min()
    data[f'life_wrange_pnpostot'] = \
        df.lifedata_postot.max() - df.lifedata_postot.min()
    data[f'life_wrange_rumtot'] = \
        df.lifedata_rumtot.max() - df.lifedata_rumtot.min()
    data[f'life_wrange_stresssev'] = \
        df.lifedata_stress.max() - df.lifedata_stress.min()

    # Calculate means for whole week
    data['life_wmean_deptot'] = df.lifedata_deptot.mean()
    data['life_wmean_fatiguetot'] = df.lifedata_fatiguetot.mean()
    data['life_wmean_pnnegtot'] = df.lifedata_negtot.mean()
    data['life_wmean_pnpostot'] = df.lifedata_postot.mean()
    data['life_wmean_rumtot'] = df.lifedata_rumtot.mean()
    data['life_wmean_stresssev'] = df.lifedata_stress.mean()

    # Calculate SD for whole week
    data['life_wsd_deptot'] = df.lifedata_deptot.std()
    data['life_wsd_fatiguetot'] = df.lifedata_fatiguetot.std()
    data['life_wsd_pnnegtot'] = df.lifedata_negtot.std()
    data['life_wsd_pnpostot'] = df.lifedata_postot.std()
    data['life_wsd_rumtot'] = df.lifedata_rumtot.std()
    data['life_wsd_stresssev'] = df.lifedata_stress.std()

    # Calculate CV for whole week
    data[f'life_wcov_deptot'] = \
        df.lifedata_deptot.std() / \
        df.lifedata_deptot.mean()
    data[f'life_wcov_fatiguetot'] = \
        df.lifedata_fatiguetot.std() / \
        df.lifedata_fatiguetot.mean()
    data[f'life_wcov_pnnegtot'] = \
        df.lifedata_negtot.std() / \
        df.lifedata_negtot.mean()
    data[f'life_wcov_pnpostot'] = \
        df.lifedata_postot.std() / \
        df.lifedata_postot.mean()
    data[f'life_wcov_rumtot'] = \
        df.lifedata_rumtot.std() / \
        df.lifedata_rumtot.mean()
    data[f'life_wcov_stresssev'] = \
        df.lifedata_stress.std() / \
        df.lifedata_stress.mean()

    # Loop days 1-7, calculate for each day
    for d in range(1, 8):
        dfd = df[df['day'] == d]

        # Count complete sessions
        data[f'life_d{d}lifesesscomp'] = \
            len(dfd[dfd['lifedata_responded'] == '1'])

        # Calculate ranges
        data[f'life_d{d}range_deptot'] = dfd.lifedata_deptot.max() - dfd.lifedata_deptot.min()
        data[f'life_d{d}range_fatiguetot'] = dfd.lifedata_fatiguetot.max() - dfd.lifedata_fatiguetot.min()
        data[f'life_d{d}range_pnnegtot'] = dfd.lifedata_negtot.max() - dfd.lifedata_negtot.min()
        data[f'life_d{d}range_pnpostot'] = dfd.lifedata_postot.max() - dfd.lifedata_postot.min()
        data[f'life_d{d}range_rumtot'] = dfd.lifedata_rumtot.max() - dfd.lifedata_rumtot.min()
        data[f'life_d{d}range_stresssev'] = dfd.lifedata_stress.max() - dfd.lifedata_stress.min()

        # Calculate means
        data[f'life_d{d}mean_deptot'] = dfd.lifedata_deptot.mean()
        data[f'life_d{d}mean_fatiguetot'] = dfd.lifedata_fatiguetot.mean()
        data[f'life_d{d}mean_pnnegtot'] = dfd.lifedata_negtot.mean()
        data[f'life_d{d}mean_pnpostot'] = dfd.lifedata_postot.mean()
        data[f'life_d{d}mean_rumtot'] = dfd.lifedata_rumtot.mean()
        data[f'life_d{d}mean_stresssev'] = dfd.lifedata_stress.mean()

        # Calculate SD
        data[f'life_d{d}sd_deptot'] = dfd.lifedata_deptot.std()
        data[f'life_d{d}sd_fatiguetot'] = dfd.lifedata_fatiguetot.std()
        data[f'life_d{d}sd_pnnegtot'] = dfd.lifedata_negtot.std()
        data[f'life_d{d}sd_pnpostot'] = dfd.lifedata_postot.std()
        data[f'life_d{d}sd_rumtot'] = dfd.lifedata_rumtot.std()
        data[f'life_d{d}sd_stresssev'] = dfd.lifedata_stress.std()

        # Calculate CV
        data[f'life_d{d}cov_deptot'] =dfd.lifedata_deptot.std() / dfd.lifedata_deptot.mean()
        data[f'life_d{d}cov_fatiguetot'] = dfd.lifedata_fatiguetot.std() / dfd.lifedata_fatiguetot.mean()
        data[f'life_d{d}cov_pnnegtot'] = dfd.lifedata_negtot.std() / dfd.lifedata_negtot.mean()
        data[f'life_d{d}cov_pnpostot'] =  dfd.lifedata_postot.std() / dfd.lifedata_postot.mean()
        data[f'life_d{d}cov_rumtot'] = dfd.lifedata_rumtot.std() / dfd.lifedata_rumtot.mean()
        data[f'life_d{d}cov_stresssev'] = dfd.lifedata_stress.std() / dfd.lifedata_stress.mean()

    # Return etl data as strings
    for k, v in data.items():
        data[k] = str(v)

    return data


def process_project(project):
    results = []
    def_field = project.def_field
    fields = [
        def_field,
        'lifedata_responded',
        'life_wlifesesscomp'
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

    # Get records
    all_records = project.export_records(fields=fields, forms=['ema_lifedata_survey'])

    # Get the repeating records
    life_records = [x for x in all_records if x['redcap_repeat_instance'] and x['redcap_repeat_instrument'] == 'ema_lifedata_survey']

    # Process each subject
    for subj in subjects:
        subj_id = subj2id[subj]
        subj_events = sorted(list(set([x['redcap_event_name'] for x in all_records if x[def_field] == subj_id])))
        subj_life = [x for x in life_records if x[def_field] == subj_id]

        # Iterate subject events
        for event_id in subj_events:

            # Find existing
            found = [x for x in all_records if (x[def_field] == subj_id) and (x['redcap_event_name'] == event_id) and (x.get('life_wlifesesscomp', False))]
            if len(found) > 0:
                logger.debug(f'found existing:{subj}:{event_id}')
                continue

            # Get repeat records
            repeats = [x for x in subj_life if (x['redcap_event_name'] == event_id)]
            if len(repeats) == 0:
                # no repeats to count
                logger.debug(f'no repeats found:{subj}:{event_id}')
                continue

            finished = [x for x in subj_life if (x['redcap_event_name'] == event_id) and (x.get('lifedata_responded', '0') == '1')]
            count_finished = len(finished)

            logger.debug(f'{subj}:{event_id}:{count_finished=}')

            data = {
                def_field: subj_id,
                'redcap_event_name': event_id,
                'life_dailyweekly_complete': '2',
            }

            if count_finished > 0:
                # Calculate daily and weekly
                data.update(extract_life(finished))
            else:
                logger.debug(f'no finished records found:{subj}:{event_id}')
                data.update({'life_wlifesesscomp': '0'})

            result = _load(project, [data])
            if result is True:
                results.append({
                    'result': 'COMPLETE',
                    'description': 'life_dailyweekly',
                    'category': 'life_dailyweekly',
                    'subject': subj_id,
                    'event': event_id
                })

    return results
