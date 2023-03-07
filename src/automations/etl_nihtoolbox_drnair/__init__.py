import logging
import tempfile

import pandas as pd

from ...utils_redcap import get_redcap, download_file, field2events


reg_field = 'toolbox_regdata'
score_field = 'toolbox_cogscores'
done_field = 'toolbox_pin'


def run(project, record_id, event_id):
    data = None

    with tempfile.TemporaryDirectory() as tmpdir:
        reg_file = f'{tmpdir}/regfile.csv'
        score_file = f'{tmpdir}/scorefile.csv'

        # Download files from redcap
        logging.debug(f'downloading file:{record_id}:{event_id}:{reg_field}:{reg_file}')
        download_file(project, record_id, event_id, reg_field, reg_file)
        logging.debug(f'downloading file:{record_id}:{event_id}:{score_field}:{score_file}')
        download_file(project, record_id, event_id, score_field, score_file)

        # Extract data from downloaded files
        reg_data = toolbox_extract_regdata(reg_file)

        score_data = toolbox_extract_cogscores(score_file)

        # Transform data to match redcap field names
        data = toolbox_transform(reg_data, score_data)

    # Load data back to redcap
    toolbox_load(project, record_id, event_id, data)


def toolbox_transform(regdata, scoredata):
    # Initialize test data
    picseqtest = None
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
    listsorttest = scoredata['NIH Toolbox List Sorting Working Memory Test Age 7+ v2.1']
    patterntest = scoredata['NIH Toolbox Pattern Comparison Processing Speed Test Age 7+ v2.1']
    picvocabtest = scoredata['NIH Toolbox Picture Vocabulary Test Age 3+ v2.1']
    oralrecogtest = scoredata['NIH Toolbox Oral Reading Recognition Test Age 3+ v2.1']
    flankertest = scoredata['NIH Toolbox Flanker Inhibitory Control and Attention Test Age 12+ v2.1']
    cardsorttest = scoredata['NIH Toolbox Dimensional Change Card Sort Test Age 12+ v2.1']

    # Get the individual scores
    data.update({
        'toolbox_listsorttest_raw': listsorttest['RawScore'],
        'toolbox_patterntest_raw': patterntest['RawScore'],
        'toolbox_picseqtest_raw': picseqtest['RawScore'],
        'toolbox_oralrecogtest_theta': oralrecogtest['Theta'],
        'toolbox_picseqtest_theta': picseqtest['Theta'],
        'toolbox_picvocabtest_theta': picvocabtest['Theta'],
        'toolbox_listsorttest_uncstd': listsorttest['Uncorrected Standard Score'],
        'toolbox_oralrecogtest_uncstd': oralrecogtest['Uncorrected Standard Score'],
        'toolbox_patterntest_uncstd': patterntest['Uncorrected Standard Score'],
        'toolbox_picseqtest_uncstd': picseqtest['Uncorrected Standard Score'],
        'toolbox_picvocabtest_uncstd': picvocabtest['Uncorrected Standard Score'],
        'toolbox_listsorttest_agestd': listsorttest['Age-Corrected Standard Score'],
        'toolbox_oralrecogtest_agestd': oralrecogtest['Age-Corrected Standard Score'],
        'toolbox_patterntest_agestd': patterntest['Age-Corrected Standard Score'],
        'toolbox_picseqtest_agestd': picseqtest['Age-Corrected Standard Score'],
        'toolbox_picvocabtest_agestd': picvocabtest['Age-Corrected Standard Score'],
        'toolbox_listsorttest_tscore': listsorttest['Fully-Corrected T-score'],
        'toolbox_oralrecogtest_tscore': oralrecogtest['Fully-Corrected T-score'],
        'toolbox_patterntest_tscore': patterntest['Fully-Corrected T-score'],
        'toolbox_picseqtest_tscore': picseqtest['Fully-Corrected T-score'],
        'toolbox_picvocabtest_tscore': picvocabtest['Fully-Corrected T-score'],
        'toolbox_flankertest_raw': flankertest['RawScore'],
        'toolbox_flankertest_uncstd': flankertest['Uncorrected Standard Score'],
        'toolbox_flankertest_agestd': flankertest['Age-Corrected Standard Score'],
        'toolbox_flankertest_tscore': flankertest['Fully-Corrected T-score'],
        'toolbox_cardsorttest_raw': cardsorttest['RawScore'],
        'toolbox_cardsorttest_uncstd': cardsorttest['Uncorrected Standard Score'],
        'toolbox_cardsorttest_agestd': cardsorttest['Age-Corrected Standard Score'],
        'toolbox_cardsorttest_tscore': cardsorttest['Fully-Corrected T-score'],
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


def toolbox_load(project, record_id, event_id, data):
    data[project.def_field] = record_id
    data['redcap_event_name'] = event_id

    try:
        response = project.import_records([data])
        assert 'count' in response
        logging.info('uploaded')
    except AssertionError as e:
        logging.error('error uploading', record_id, e)


def toolbox_extract_regdata(filename):
    data = {}

    try:
        df = pd.read_csv(filename)
    except:
        df = pd.read_excel(filename)

    # Get data from last row
    data = df.iloc[-1].to_dict()

    return data


def toolbox_extract_cogscores(filename):
    data = {}

    # Load csv
    try:
        df = pd.read_csv(filename)
    except:
        df = pd.read_excel(filename)

    # Drop instrument duplicates, keeping the last only
    df = df.drop_duplicates(subset='Inst', keep='last')

    # convert to dict of dicts indexed by Instrument
    df = df.dropna(subset=['Inst'])
    df = df.set_index('Inst')
    data = df.to_dict('index')

    return data


def process_project(project):
    results = []
    events = field2events(project, reg_field)

    records = project.export_records(
        fields=[project.def_field, done_field, reg_field, score_field],
        events=events)

    for r in records:
        record_id = r[project.def_field]
        event_id = r['redcap_event_name']

        if r[done_field]:
            logging.debug(f'already ETL:{record_id}:{event_id}')
            continue

        if not r[score_field]:
            logging.debug(f'no data file:{record_id}:{event_id}')
            continue

        logging.info(f'running ETL:{record_id}:{event_id}')
        results.append({'subject': record_id, 'event': event_id})
        run(project, record_id, event_id)

    return results


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    process_project(get_redcap('', api_key=''))
    print('Done!')
