"""Gaitrite data extraction."""
import logging
import os

import pandas as pd


logger = logging.getLogger('garjus.automations.etl_gaitrite')

# VUMC - all 4 walk types in a single excel file. column indicates walk type

# UPMC - file per walk type

# TODO: add Stride Velocity L/R mean and  Stride Velocity L/R stddev


def process(gaitrite_file):
    """Process Gaitrite file and return subset of data."""
    data = []

    if gaitrite_file.endswith('.zip'):
        logger.error('ignoring zip file')
        return []

    # Load each test, extract returns an array of tests.
    for d in _extract(gaitrite_file):
        data.append({
            'gaitrite_testrecord': str(d['Test Record #']),
            'gaitrite_datetime': str(d['Date / Time of Test']),
            'gaitrite_comments': str(d['Comments']),
            'gaitrite_velocity': str(d['Velocity']),
            'gaitrite_cadence': str(d['Cadence']),
            'gaitrite_steptime_left': str(d['Step Time(sec) L']),
            'gaitrite_steptime_right': str(d['Step Time(sec) R']),
            'gaitrite_stepextremity_left': str(d['Step Extremity(ratio) L']),
            'gaitrite_stepextremity_right': str(d['Step Extremity(ratio) R']),
            'gaitrite_stridelen_left': str(d['Stride Length(cm) L']),
            'gaitrite_stridelen_right': str(d['Stride Length(cm) R']),
            'gaitrite_swingtime_left': str(d['Swing Time(sec) L']),
            'gaitrite_swingtime_right': str(d['Swing Time(sec) R']),
            'gaitrite_stancetime_left': str(d['Stance Time(sec)  L']),
            'gaitrite_stancetime_right': str(d['Stance Time(sec)  R']),
            'gaitrite_funcambprofile': str(d['Functional Amb. Profile']),
            'gaitrite_normvelocity': str(d['Normalized Velocity  ']),
            'gaitrite_heeltime_left': str(d['Heel Off On Time L']),
            'gaitrite_heeltime_right': str(d['Heel Off On Time R'])
        })

    return data


def process_upmc(filename):
    """Process file and return subset of data."""

    # Load each test, extract returns an array of tests.
    d = _extract_upmc(filename)

    return {
        'gaitrite_datetime': str(d['Date / Time of Test']),
        'gaitrite_comments': str(d['Comments']),
        'gaitrite_velocity': str(d['Velocity']),
        'gaitrite_cadence': str(d['Cadence']),
        'gaitrite_steptime_left': str(d['Step Time(sec) L']),
        'gaitrite_steptime_right': str(d['Step Time(sec) R']),
        'gaitrite_stridelen_left': str(d['Stride Length(cm) L']),
        'gaitrite_stridelen_right': str(d['Stride Length(cm) R']),
        'gaitrite_swingtime_left': str(d['Swing Time(sec) L']),
        'gaitrite_swingtime_right': str(d['Swing Time(sec) R']),
        'gaitrite_stancetime_left': str(d['Stance Time(sec)  L']),
        'gaitrite_stancetime_right': str(d['Stance Time(sec)  R']),
    }


def _extract_upmc(filename):
    data = {}

    # First load from the top section of the file
    df = pd.read_excel(
        filename,
        header=None,
        usecols=[0,1],
        nrows=9,
        names=['FieldName', 'FieldValue'],

    )
    data['Comments'] = df[df['FieldName'] == 'Memo'].iloc[0].FieldValue
    data['Date / Time of Test'] = df[df['FieldName'] == 'Test Time'].iloc[0].FieldValue

    # Then load spreadsheet section
    df = pd.read_excel(filename, skiprows=11)
    df = df.fillna('')
    df = df.rename(columns={'Unnamed: 0': 'Measure', 'Unnamed: 1': 'LR'})

    try:
        data['Velocity'] = df[(df['Measure'] == 'Mean') & (df['LR'] == '')].iloc[0]['Velocity (cm./sec.)']
        data['Cadence'] = df[(df['Measure'] == 'Mean') & (df['LR'] == '')].iloc[0]['Cadence (steps/min.)']
        data['Step Time(sec) L'] = df[(df['Measure'] == 'Mean') & (df['LR'] == 'Left')].iloc[0]['Step Time (sec.)']
        data['Step Time(sec) R'] = df[(df['Measure'] == 'Mean') & (df['LR'] == 'Right')].iloc[0]['Step Time (sec.)']
        data['Stride Length(cm) L'] = df[(df['Measure'] == 'Mean') & (df['LR'] == 'Left')].iloc[0]['Stride Length (cm.)']
        data['Stride Length(cm) R'] = df[(df['Measure'] == 'Mean') & (df['LR'] == 'Right')].iloc[0]['Stride Length (cm.)']
        data['Swing Time(sec) L'] = df[(df['Measure'] == 'Mean') & (df['LR'] == 'Left')].iloc[0]['Swing Time (sec.)']
        data['Swing Time(sec) R'] = df[(df['Measure'] == 'Mean') & (df['LR'] == 'Right')].iloc[0]['Swing Time (sec.)']
        data['Stance Time(sec)  L'] = df[(df['Measure'] == 'Mean') & (df['LR'] == 'Left')].iloc[0]['Stance Time (sec.)']
        data['Stance Time(sec)  R'] = df[(df['Measure'] == 'Mean') & (df['LR'] == 'Right')].iloc[0]['Stance Time (sec.)']
    except Exception as err:
        print(err)

    return data


def _extract(filename):
    """ Extract data from file that has a header row and one data row"""
    try:
        df = pd.read_csv(filename, dtype=str)
    except Exception:
        df = pd.read_excel(filename, dtype=str)

    try:
        df = df.dropna(subset=['Test Record #'])
    except Exception as err:
        logger.error(f'failed to extract gaitrite from excel:{err}')
        return []

    df = df.sort_values('Test Record #')

    # Fill nan with blanks
    df = df.fillna('')

    return df.to_records()


if __name__ == "__main__":
    import pprint

    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    _dir = os.path.expanduser('~/Downloads')
    test_file = f'{_dir}/V1099_Gaitrite_Baseline.xlsx'

    data = process(test_file)
    pprint.pprint(data)
