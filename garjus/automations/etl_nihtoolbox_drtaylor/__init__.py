import logging
import pandas as pd


logger = logging.getLogger('garjus.automations.etl_nihtoolbox_drtaylor')


def process(reg_file, score_file):
    # Extract data from downloaded files
    reg_data = _extract_regdata(reg_file)
    score_data = _extract_cogscores(score_file)

    # Transform data to match desired field names
    data = _transform(reg_data, score_data)

    return data


def _transform(regdata, scoredata):
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
    })

    cogcrystalcomp = scoredata.get('Cognition Crystallized Composite v1.1', None)
    #audlearntest = scoredata.get('NIH Toolbox Auditory Verbal Learning Test (Rey) Age 8+ v2.0', None)

    #if audlearntest:
    #    data.update({
    #        'toolbox_audlearntest_raw': audlearntest['RawScore'],
    #    })

    if cogcrystalcomp:
        data.update({
            'toolbox_cogcrystalcomp_uncstd': cogcrystalcomp['Uncorrected Standard Score'],
            'toolbox_cogcrystalcomp_agestd': cogcrystalcomp['Age-Corrected Standard Score'],
            'toolbox_cogcrystalcomp_tscore': cogcrystalcomp['Fully-Corrected T-score'],
        })

    return data


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
    data = df.to_dict('index')

    return data
