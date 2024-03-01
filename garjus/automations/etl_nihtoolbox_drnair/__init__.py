import logging
import tempfile

import pandas as pd

from ...utils_redcap import get_redcap, download_file, field2events


logger = logging.getLogger('garjus.automations.etl_nihtoolbox_drnair')


reg_field = 'toolbox_regdata'
score_field = 'toolbox_cogscores'
done_field = 'toolbox_pin'


def process(project):
    results = []
    events = field2events(project, reg_field)

    records = project.export_records(
        fields=[project.def_field, done_field, reg_field, score_field],
        events=events)

    for r in records:
        record_id = r[project.def_field]
        event_id = r['redcap_event_name']

        if r[done_field]:
            logger.debug(f'already ETL:{record_id}:{event_id}')
            continue

        if not r[reg_field]:
            logger.debug(f'no reg file:{record_id}:{event_id}')
            continue

        if not r[score_field]:
            logger.debug(f'no data file:{record_id}:{event_id}')
            continue

        logger.debug(f'running ETL:{record_id}:{event_id}')
        results.append({'subject': record_id, 'event': event_id})
        _run(project, record_id, event_id)

    return results


def _run(project, record_id, event_id):
    data = None

    with tempfile.TemporaryDirectory() as tmpdir:
        reg_file = f'{tmpdir}/regfile.csv'
        score_file = f'{tmpdir}/scorefile.csv'

        # Download files from redcap
        logger.debug(f'downloading file:{record_id}:{event_id}:{reg_field}:{reg_file}')
        download_file(project, record_id, reg_field, reg_file, event_id=event_id)
        logger.debug(f'downloading file:{record_id}:{event_id}:{score_field}:{score_file}')
        download_file(project, record_id, score_field, score_file, event_id=event_id)

        # Extract data from downloaded files
        reg_data = _extract_regdata(reg_file)

        score_data = _extract_cogscores(score_file)

        # Transform data to match redcap field names
        data = _transform(reg_data, score_data)

    # Load data back to redcap
    print(data)
    _load(project, record_id, event_id, data)


def _transform(regdata, scoredata):
    # Initialize test data
    picseqtest = {}
    listsorttest = None
    patterntest = None
    picvocabtest = None
    oralrecogtest = None
    cogcrystalcomp = None
    data = {}

    # Start with the registration data
    data.update({
        'toolbox_pin': regdata['PIN'],
        'toolbox_deviceid': regdata['DeviceID'],
        'toolbox_age': regdata['Age'],
        'toolbox_education': regdata['Education'],
        'toolbox_gender': regdata['Gender'],
        'toolbox_handedness': regdata['Handedness'],
        'toolbox_race': regdata['Race'],
        'toolbox_ethnicity': regdata['Ethnicity'],
        'toolbox_assessment': regdata['Assessment Name'],
    })

    # Find the Pic Seq data that has mutliple versions
    for i in list(scoredata.keys()):
        if i.startswith('NIH Toolbox Picture Sequence Memory Test'):
            picseqtest = scoredata[i]

    # Load the other tests
    listsorttest = scoredata.get('NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1', {})
    patterntest = scoredata.get('NIH Toolbox Pattern Comparison Processing Speed Test Age 7+ v2.1', {})
    picvocabtest = scoredata.get('NIH Toolbox Picture Vocabulary Test Age 3+ v2.1', {})
    oralrecogtest = scoredata.get('NIH Toolbox Oral Reading Recognition Test Age 3+ v2.1', {})
    flankertest = scoredata.get('NIH Toolbox Flanker Inhibitory Control and Attention Test Age 12+ v2.1', {})
    cardsorttest = scoredata.get('NIH Toolbox Dimensional Change Card Sort Test Age 12+ v2.1', {})

    # Get the individual scores
    data.update({
        'toolbox_listsorttest_raw': listsorttest.get('RawScore', ''),
        'toolbox_patterntest_raw': patterntest.get('RawScore', ''),
        'toolbox_picseqtest_raw': picseqtest.get('RawScore', ''),
        'toolbox_oralrecogtest_theta': oralrecogtest.get('Theta', ''),
        'toolbox_picseqtest_theta': picseqtest.get('Theta', ''),
        'toolbox_picvocabtest_theta': picvocabtest.get('Theta', ''),
        'toolbox_listsorttest_uncstd': listsorttest.get('Uncorrected Standard Score', ''),
        'toolbox_oralrecogtest_uncstd': oralrecogtest.get('Uncorrected Standard Score', ''),
        'toolbox_patterntest_uncstd': patterntest.get('Uncorrected Standard Score', ''),
        'toolbox_picseqtest_uncstd': picseqtest.get('Uncorrected Standard Score', ''),
        'toolbox_picvocabtest_uncstd': picvocabtest.get('Uncorrected Standard Score', ''),
        'toolbox_listsorttest_agestd': listsorttest.get('Age-Corrected Standard Score', ''),
        'toolbox_oralrecogtest_agestd': oralrecogtest.get('Age-Corrected Standard Score', ''),
        'toolbox_patterntest_agestd': patterntest.get('Age-Corrected Standard Score', ''),
        'toolbox_picseqtest_agestd': picseqtest.get('Age-Corrected Standard Score', ''),
        'toolbox_picvocabtest_agestd': picvocabtest.get('Age-Corrected Standard Score', ''),
        'toolbox_listsorttest_tscore': listsorttest.get('Fully-Corrected T-score', ''),
        'toolbox_oralrecogtest_tscore': oralrecogtest.get('Fully-Corrected T-score', ''),
        'toolbox_patterntest_tscore': patterntest.get('Fully-Corrected T-score', ''),
        'toolbox_picseqtest_tscore': picseqtest.get('Fully-Corrected T-score', ''),
        'toolbox_picvocabtest_tscore': picvocabtest.get('Fully-Corrected T-score', ''),
        'toolbox_flankertest_raw': flankertest.get('RawScore', ''),
        'toolbox_flankertest_uncstd': flankertest.get('Uncorrected Standard Score', ''),
        'toolbox_flankertest_agestd': flankertest.get('Age-Corrected Standard Score', ''),
        'toolbox_flankertest_tscore': flankertest.get('Fully-Corrected T-score', ''),
        'toolbox_cardsorttest_raw': cardsorttest.get('RawScore', ''),
        'toolbox_cardsorttest_uncstd': cardsorttest.get('Uncorrected Standard Score', ''),
        'toolbox_cardsorttest_agestd': cardsorttest.get('Age-Corrected Standard Score', ''),
        'toolbox_cardsorttest_tscore': cardsorttest.get('Fully-Corrected T-score', ''),
    })

    cogcrystalcomp = scoredata.get('Cognition Crystallized Composite v1.1', None)
    cogfluidcomp = scoredata.get('Cognition Fluid Composite v1.1', None)
    cogearlycomp = scoredata.get('Cognition Early Childhood Composite v1.1', None)
    cogtotalcomp = scoredata.get('Cognition Total Composite Score v1.1', None)
    audlearntest = scoredata.get('NIH Toolbox Auditory Verbal Learning Test (Rey) Age 8+ v2.0', None)

    if audlearntest:
        data.update({
            'toolbox_audlearntest_raw': audlearntest['RawScore'],
        })

    if cogcrystalcomp:
        data.update({
            'toolbox_cogcrystalcomp_uncstd': cogcrystalcomp['Uncorrected Standard Score'],
            'toolbox_cogcrystalcomp_agestd': cogcrystalcomp['Age-Corrected Standard Score'],
            'toolbox_cogcrystalcomp_tscore': cogcrystalcomp['Fully-Corrected T-score'],
        })

    if cogfluidcomp:
        data.update({
            'toolbox_cogfluidcomp_uncstd': cogfluidcomp['Uncorrected Standard Score'],
            'toolbox_cogfluidcomp_agestd': cogfluidcomp['Age-Corrected Standard Score'],
            'toolbox_cogfluidcomp_tscore': cogfluidcomp['Fully-Corrected T-score'],
        })

    if cogearlycomp:
        data.update({
            'toolbox_cogearlycomp_uncstd': cogearlycomp['Uncorrected Standard Score'],
            'toolbox_cogearlycomp_agestd': cogearlycomp['Age-Corrected Standard Score'],
            'toolbox_cogearlycomp_tscore': cogearlycomp['Fully-Corrected T-score'],
        })

    if cogearlycomp:
        data.update({
            'toolbox_cogtotalcomp_uncstd': cogtotalcomp['Uncorrected Standard Score'],
            'toolbox_cogtotalcomp_agestd': cogtotalcomp['Age-Corrected Standard Score'],
            'toolbox_cogtotalcomp_tscore': cogtotalcomp['Fully-Corrected T-score'],
        })

    return data


def _load(project, record_id, event_id, data):



    data[project.def_field] = record_id
    data['redcap_event_name'] = event_id

    try:
        response = project.import_records([data])
        assert 'count' in response
        logger.debug('uploaded')
    except AssertionError as e:
        logger.error('error uploading', record_id, e)


def _extract_regdata(filename):
    data = {}

    try:
        df = pd.read_csv(filename)
    except Exception:
        df = pd.read_excel(filename)

    # Get data from last row
    data = df.iloc[-1].to_dict()

    return data


def _extract_cogscores(filename):
    data = {}

    # Load csv
    try:
        df = pd.read_csv(filename)
    except Exception:
        df = pd.read_excel(filename)

    # Drop instrument duplicates, keeping the last only
    df = df.drop_duplicates(subset='Inst', keep='last')

    # convert to dict of dicts indexed by Instrument
    df = df.dropna(subset=['Inst'])
    df = df.set_index('Inst')
    df = df.fillna('')
    data = df.to_dict('index')

    return data
