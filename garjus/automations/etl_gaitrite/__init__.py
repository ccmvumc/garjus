"""Gaitrite data extraction."""
import logging
import os

import pandas as pd


logger = logging.getLogger('garjus.automations.etl_gaitrite')


def process(gaitrite_file):
    """Process Gaitrite file and return subset of data."""
    data = []

    # Load each test
    for d in _extract(gaitrite_file):
        data.append({
            'gaitrite_testrecord': d['Test Record #'],
            'gaitrite_datetime': d['Date / Time of Test'],
            'gaitrite_comments': d['Comments'],
            'gaitrite_velocity': d['Velocity'],
            'gaitrite_cadence': d['Cadence'],
            'gaitrite_steptime_left': d['Step Time(sec) L'],
            'gaitrite_steptime_right': d['Step Time(sec) R'],
            'gaitrite_stepextremity_left': d['Step Extremity(ratio) L'],
            'gaitrite_stepextremity_right': d['Step Extremity(ratio) R'],
            'gaitrite_stridelen_left': d['Stride Length(cm) L'],
            'gaitrite_stridelen_right': d['Stride Length(cm) R'],
            'gaitrite_swingtime_left': d['Swing Time(sec) L'],
            'gaitrite_swingtime_right': d['Swing Time(sec) R'],
            'gaitrite_stancetime_left': d['Stance Time(sec)  L'],
            'gaitrite_stancetime_right': d['Stance Time(sec)  R'],
            'gaitrite_funcambprofile': d['Functional Amb. Profile'],
            'gaitrite_normvelocity': d['Normalized Velocity  '],
            'gaitrite_heeltime_left': d['Heel Off On Time L'],
            'gaitrite_heeltime_right': d['Heel Off On Time R']
        })

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
