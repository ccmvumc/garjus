import logging
import sys
import redcap
import tempfile

from .process import process
from ...utils_redcap import download_file, field2events


logger = logging.getLogger('garjus.automations.etl_nihexaminer')



def run(project):
    """Process examiner files from REDCap and upload results."""
    data = {}
    results = []
    events = []
    fields = []
    records = []
    flank_field = 'flanker_file'
    nback_field = 'nback_upload'
    shift_field = 'set_shifting_file'
    cpt_field = 'cpt_upload'
    done_field = 'flanker_score'

    if 'flanker_summfile' in project.field_names:
        # Alternate file field names
        flank_field = 'flanker_summfile'
        nback_field = 'nback_summfile'
        shift_field = 'set_shifting_summfile'
        cpt_field = 'cpt_summfile'

    # Get the fields
    fields = [
        project.def_field,
        done_field,
        cpt_field,
        nback_field,
        shift_field,
        flank_field,
        'dot_count_tot',
        'anti_trial_1',
        'anti_trial_2',
        'correct_f',
        'correct_l',
        'correct_animal',
        'correct_veg',
        'repetition_f',
        'rule_vio_f',
        'repetition_l',
        'rule_vio_l',
        'repetition_animal',
        'rule_vio_animal',
        'repetition_veg',
        'rule_vio_veg',
        'brs_1',
        'brs_2',
        'brs_3',
        'brs_4',
        'brs_5',
        'brs_6',
        'brs_7',
        'brs_8',
        'brs_9',
    ]

    if 'correct_s' in project.field_names:
        fields.extend([
            'correct_s', 'rule_vio_s', 'repetition_s',
            'correct_t', 'rule_vio_t', 'repetition_t',
            'correct_fruit', 'rule_vio_fruit', 'repetition_fruit',
            'correct_r', 'rule_vio_r', 'repetition_r',
            'correct_m', 'rule_vio_m', 'repetition_m',
            'correct_cloth', 'rule_vio_cloth', 'repetition_cloth',
        ])

    # Determine events
    events = field2events(project, cpt_field)

    # Get records for those events and fields
    records = project.export_records(fields=fields, events=events)

    for r in records:
        data = {}
        record_id = r[project.def_field]
        event_id = r['redcap_event_name']

        if r[done_field]:
            logger.debug(f'already ETL:{record_id}:{event_id}')
            continue

        if not r[cpt_field]:
            logger.debug(f'no data file:{record_id}:{event_id}')
            continue

        # Check for blanks
        has_blank = False
        check_fields = [
            flank_field,
            nback_field,
            shift_field,
            cpt_field,
            'dot_count_tot',
            'anti_trial_1',
            'anti_trial_2',
            'correct_f',
            'correct_l',
            'correct_animal',
            'correct_veg',
            'repetition_f',
            'rule_vio_f',
            'repetition_l',
            'rule_vio_l',
            'repetition_animal',
            'rule_vio_animal',
            'repetition_veg',
            'rule_vio_veg',
            'brs_1',
            'brs_2',
            'brs_3',
            'brs_4',
            'brs_5',
            'brs_6',
            'brs_7',
            'brs_8',
            'brs_9']

        for k in check_fields:
            if r[k] == '' and k != done_field:
                logger.debug(f'blank value:{record_id}:{event_id}:{k}')
                has_blank = True
                break

        if has_blank:
            continue

        logger.debug(f'running nihexaminer ETL:{record_id}:{event_id}')

        # Get values needed for scoring
        manual_values = {
            'dot_total': int(r['dot_count_tot']),
            'anti_trial_1': int(r['anti_trial_1']),
            'anti_trial_2': int(r['anti_trial_2']),
            'cf1_corr': int(r['correct_animal']),
            'cf1_rep': int(r['repetition_animal']),
            'cf1_rv': int(r['rule_vio_animal']),
            'brs_1': int(r['brs_1']),
            'brs_2': int(r['brs_2']),
            'brs_3': int(r['brs_3']),
            'brs_4': int(r['brs_4']),
            'brs_5': int(r['brs_5']),
            'brs_6': int(r['brs_6']),
            'brs_7': int(r['brs_7']),
            'brs_8': int(r['brs_8']),
            'brs_9': int(r['brs_9']),
        }

        if r['correct_f']:
            # examiner version 0
            manual_values.update({
                'vf1_corr': int(r['correct_f']),
                'vf1_rep': int(r['repetition_f']),
                'vf1_rv': int(r['rule_vio_f']),
                'vf2_corr': int(r['correct_l']),
                'vf2_rep': int(r['repetition_l']),
                'vf2_rv': int(r['rule_vio_l']),
                'cf2_corr': int(r['correct_veg']),
                'cf2_rep': int(r['repetition_veg']),
                'cf2_rv': int(r['rule_vio_veg'])
            })
        elif r['correct_t']:
            # examiner version 1
            manual_values.update({
                'vf1_corr': int(r['correct_t']),
                'vf1_rep': int(r['repetition_t']),
                'vf1_rv': int(r['rule_vio_t']),
                'vf2_corr': int(r['correct_s']),
                'vf2_rep': int(r['repetition_s']),
                'vf2_rv': int(r['rule_vio_s']),
                'cf2_corr': int(r['correct_fruit']),
                'cf2_rep': int(r['repetition_fruit']),
                'cf2_rv': int(r['rule_vio_fruit'])
            })
        else:
            # examiner version 2
            manual_values.update({
                'vf1_corr': int(r['correct_r']),
                'vf1_rep': int(r['repetition_r']),
                'vf1_rv': int(r['rule_vio_r']),
                'vf2_corr': int(r['correct_m']),
                'vf2_rep': int(r['repetition_m']),
                'vf2_rv': int(r['rule_vio_m']),
                'cf2_corr': int(r['correct_cloth']),
                'cf2_rep': int(r['repetition_cloth']),
                'cf2_rv': int(r['rule_vio_cloth'])
            })

        with tempfile.TemporaryDirectory() as tmpdir:
            # Get files needed
            flank_file = f'{tmpdir}/flanker.csv'
            cpt_file = f'{tmpdir}/cpt.csv'
            nback_file = f'{tmpdir}/nback.csv'
            shift_file = f'{tmpdir}/shift.csv'

            try:
                # Download files from redcap
                logger.debug(f'download files:{record_id}:{event_id}:{flank_file}')
                download_file(project, record_id, flank_field, flank_file, event_id=event_id)
                logger.debug(f'download NBack:{record_id}:{event_id}:{nback_field}')
                download_file(project, record_id, nback_field, nback_file, event_id=event_id)
                logger.debug(f'download Shift:{record_id}:{event_id}:{shift_field}')
                download_file(project, record_id, shift_field, shift_file, event_id=event_id)
                logger.debug(f'download CPT:{record_id}:{event_id}:{cpt_field}')
                download_file(project, record_id, cpt_field, cpt_file, event_id=event_id)
            except Exception as err:
                logger.error(f'downloading files:{record_id}:{event_id}')
                continue

            try:
                # Process inputs
                data = process(
                    manual_values,
                    flank_file,
                    cpt_file,
                    nback_file,
                    shift_file)
            except Exception as err:
                logger.error(f'processing examiner:{record_id}:{event_id}:{err}')
                continue

        # Load data back to redcap
        _load(project, record_id, event_id, data)
        results.append({'subject': record_id, 'event': event_id})

    return results