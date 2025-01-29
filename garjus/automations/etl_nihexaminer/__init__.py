import logging
import sys
import redcap
import tempfile

from .process import process
from ...utils_redcap import download_file, field2events
from .file2redcap import process_project as file2redcap

logger = logging.getLogger('garjus.automations.etl_nihexaminer')


COGD_FIELDS = [
    'nih_dot_total',
    'nih_anti_1',
    'nih_anti_2',
    'nih_f_total',
    'nih_f_rep',
    'nih_f_rv',
    'nih_l_total',
    'nih_l_rep',
    'nih_l_rv',
    'nih_animals_cor',
    'nih_animal_rep',
    'nih_animals_rv',
    'nih_veg_cor',
    'nih_veg_rep',
    'nih_veg_rv',
    'nih_agitation',
    'nih_stim_bound',
    'nih_persev',
    'nih_initiation',
    'nih_motor',
    'nih_distract',
    'nih_engage',
    'nih_impulsivity',
    'nih_social',
    'nih_dot_total_v2',
    'nih_anti_1_v2',
    'nih_anti_2_v2',
    'nih_t_total_v2',
    'nih_t_rep_v2',
    'nih_t_rv_v2',
    'nih_s_total_v2',
    'nih_s_rep_v2',
    'nih_s_rv_v2',
    'nih_animals_cor_v2',
    'nih_animal_rep_v2',
    'nih_animals_rv_v2',
    'nih_fruit_cor_v2',
    'nih_fruit_rep_v2',
    'nih_fruit_rv_v2',
    'nih_agitation_v2',
    'nih_stim_bound_v2',
    'nih_persev_v2',
    'nih_initiation_v2',
    'nih_motor_v2',
    'nih_distract_v2',
    'nih_engage_v2',
    'nih_impulsivity_v2',
    'nih_social_v2',
    'nih_dot_total_v3',
    'nih_anti_1_v3',
    'nih_anti_2_v3',
    'nih_r_total_v3',
    'nih_r_rep_v3',
    'nih_r_rv_v3',
    'nih_m_total_v3',
    'nih_m_rep_v3',
    'nih_m_rv_v3',
    'nih_animals_cor_v3',
    'nih_animal_rep_v3',
    'nih_animals_rv_v3',
    'nih_cloth_cor_v3',
    'nih_cloth_rep_v3',
    'nih_cloth_rv_v3',
    'nih_agitation_v3',
    'nih_stim_bound_v3',
    'nih_persev_v3',
    'nih_initiation_v3',
    'nih_motor_v3',
    'nih_distract_v3',
    'nih_engage_v3',
    'nih_impulsivity_v3',
    'nih_social_v3',
]

D3_FIELDS = [
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


def _load(project, record_id, event_id, data, repeat_id=None):
    data[project.def_field] = record_id
    data['redcap_event_name'] = event_id
    data = {k: str(v) for k, v in data.items()}

    if repeat_id:
        data['redcap_repeat_instance'] = repeat_id

    try:
        response = project.import_records([data])
        assert 'count' in response
        logger.debug(f'uploaded:{record_id}:{event_id}')
    except AssertionError as e:
        logger.error('error uploading', record_id, e)


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
    ]

    if 'dot_count_tot' in project.field_names:
        fields.extend(D3_FIELDS)
    else:
        fields.extend(COGD_FIELDS)

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

        try:

            if r[done_field]:
                logger.debug(f'already ETL:{record_id}:{event_id}')
                continue

            if not r[cpt_field]:
                logger.debug(f'no data file:{record_id}:{event_id}')
                continue

            logger.debug(f'running nihexaminer ETL:{record_id}:{event_id}')

            # Get values needed for scoring
            if r.get('nih_dot_total', False):
                manual_values = {
                    'dot_total': int(r['nih_dot_total']),
                    'anti_trial_1': int(r['nih_anti_1']),
                    'anti_trial_2': int(r['nih_anti_2']),
                    'cf1_corr': int(r['nih_animals_cor']),
                    'cf1_rep': int(r['nih_animal_rep']),
                    'cf1_rv': int(r['nih_animals_rv']),
                    'vf1_corr': int(r['nih_f_total']),
                    'vf1_rep': int(r['nih_f_rep']),
                    'vf1_rv': int(r['nih_f_rv']),
                    'vf2_corr': int(r['nih_l_total']),
                    'vf2_rep': int(r['nih_l_rep']),
                    'vf2_rv': int(r['nih_l_rv']),
                    'cf2_corr': int(r['nih_veg_cor']),
                    'cf2_rep': int(r['nih_veg_rep']),
                    'cf2_rv': int(r['nih_veg_rv']),
                    'brs_1': int(r['nih_agitation']),
                    'brs_2': int(r['nih_stim_bound']),
                    'brs_3': int(r['nih_persev']),
                    'brs_4': int(r['nih_initiation']),
                    'brs_5': int(r['nih_motor']),
                    'brs_6': int(r['nih_distract']),
                    'brs_7': int(r['nih_engage']),
                    'brs_8': int(r['nih_impulsivity']),
                    'brs_9': int(r['nih_social']),
                }
            elif r.get('nih_dot_total_v2', False):
                manual_values = {
                    'dot_total': int(r['nih_dot_total_v2']),
                    'anti_trial_1': int(r['nih_anti_1_v2']),
                    'anti_trial_2': int(r['nih_anti_2_v2']),
                    'cf1_corr': int(r['nih_animals_cor_v2']),
                    'cf1_rep': int(r['nih_animal_rep_v2']),
                    'cf1_rv': int(r['nih_animals_rv_v2']),
                    'vf1_corr': int(r['nih_t_total_v2']),
                    'vf1_rep': int(r['nih_t_rep_v2']),
                    'vf1_rv': int(r['nih_t_rv_v2']),
                    'vf2_corr': int(r['nih_s_total_v2']),
                    'vf2_rep': int(r['nih_s_rep_v2']),
                    'vf2_rv': int(r['nih_s_rv_v2']),
                    'cf2_corr': int(r['nih_fruit_cor_v2']),
                    'cf2_rep': int(r['nih_fruit_rep_v2']),
                    'cf2_rv': int(r['nih_fruit_rv_v2']),
                    'brs_1': int(r['nih_agitation_v2']),
                    'brs_2': int(r['nih_stim_bound_v2']),
                    'brs_3': int(r['nih_persev_v2']),
                    'brs_4': int(r['nih_initiation_v2']),
                    'brs_5': int(r['nih_motor_v2']),
                    'brs_6': int(r['nih_distract_v2']),
                    'brs_7': int(r['nih_engage_v2']),
                    'brs_8': int(r['nih_impulsivity_v2']),
                    'brs_9': int(r['nih_social_v2']),
                }
            elif r.get('nih_dot_total_v3', False):
                manual_values = {
                    'dot_total': int(r['nih_dot_total_v3']),
                    'anti_trial_1': int(r['nih_anti_1_v3']),
                    'anti_trial_2': int(r['nih_anti_2_v3']),
                    'cf1_corr': int(r['nih_animals_cor_v3']),
                    'cf1_rep': int(r['nih_animal_rep_v3']),
                    'cf1_rv': int(r['nih_animals_rv_v3']),
                    'vf1_corr': int(r['nih_r_total_v3']),
                    'vf1_rep': int(r['nih_r_rep_v3']),
                    'vf1_rv': int(r['nih_r_rv_v3']),
                    'vf2_corr': int(r['nih_m_total_v3']),
                    'vf2_rep': int(r['nih_m_rep_v3']),
                    'vf2_rv': int(r['nih_m_rv_v3']),
                    'cf2_corr': int(r['nih_cloth_cor_v3']),
                    'cf2_rep': int(r['nih_cloth_rep_v3']),
                    'cf2_rv': int(r['nih_cloth_rv_v3']),
                    'brs_1': int(r['nih_agitation_v3']),
                    'brs_2': int(r['nih_stim_bound_v3']),
                    'brs_3': int(r['nih_persev_v3']),
                    'brs_4': int(r['nih_initiation_v3']),
                    'brs_5': int(r['nih_motor_v3']),
                    'brs_6': int(r['nih_distract_v3']),
                    'brs_7': int(r['nih_engage_v3']),
                    'brs_8': int(r['nih_impulsivity_v3']),
                    'brs_9': int(r['nih_social_v3']),
                }
            elif r.get('dot_count_tot', False):
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

                if r.get('correct_f', False):
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
                elif r.get('correct_t', False):
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
            else:
                logger.error(f'manual values not found:{record_id}:{event_id}')
                continue

        except (KeyError, ValueError) as err:
            logger.error(f'value error, cannot load{err}')
            continue

        if not r.get(nback_field, False):
            logger.info(f'File Not Found NBack:{record_id}:{event_id}')
            continue

        with tempfile.TemporaryDirectory() as tmpdir:
            # Get files needed
            flank_file = f'{tmpdir}/flanker.csv'
            cpt_file = f'{tmpdir}/cpt.csv'
            nback_file = f'{tmpdir}/nback.csv'
            shift_file = f'{tmpdir}/shift.csv'

            try:
                # Download files from redcap
                logger.debug(f'download Flanker:{record_id}:{event_id}')
                download_file(
                    project,
                    record_id,
                    flank_field,
                    flank_file,
                    event_id=event_id
                )
                
                logger.debug(f'download NBack:{record_id}:{event_id}')
                download_file(
                    project,
                    record_id,
                    nback_field,
                    nback_file,
                    event_id=event_id
                )

                logger.debug(f'download Shift:{record_id}:{event_id}')
                download_file(
                    project,
                    record_id,
                    shift_field,
                    shift_file,
                    event_id=event_id
                )

                logger.debug(f'download CPT:{record_id}:{event_id}')
                download_file(
                    project,
                    record_id,
                    cpt_field,
                    cpt_file,
                    event_id=event_id
                )

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
                    shift_file
                )
            except Exception as err:
                logger.error(f'examiner:{record_id}:{event_id}:{err}')
                continue

        # Load data back to redcap
        logger.debug(f'loading:{project}:{record_id}:{event_id}')

        if 'nih_dot_total' in project.field_names:
            data['nih_examiner_scoring_complete'] = '2'
        
        repeat_id = r.get('redcap_repeat_instance', None)
        _load(project, record_id, event_id, data, repeat_id=repeat_id)

        # Save results
        results.append({
            'subject': record_id,
            'event': event_id,
            'result': 'COMPLETE',
            'category': 'etl_nihexaminer',
            'description': 'etl_nihexaminer',
        })

    return results
