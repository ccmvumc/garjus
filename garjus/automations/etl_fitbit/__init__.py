"""Fitbit data extraction."""
import logging
import os
import tempfile

import pandas as pd


logger = logging.getLogger('garjus.automations.etl_fitbit')


def process(data_file):
    """Process file and return subset of data."""
    data = {}

    # Extract data from file
    df = _extract(data_file)

    # Get days with at least 50% worn
    df = df[df['Percentage Worn'].astype(float) >= 50.0]

    # Count the days worn
    data['fitbit_daysworn'] = len(df)

    return data


def _extract(filename):
    """ Extract data from file that has a header row and one data row"""
    try:
        df = pd.read_csv(filename, dtype=str)
    except Exception:
        df = pd.read_excel(filename, dtype=str)

    try:
        df = df.dropna(subset=['Percentage Worn'])
    except Exception as err:
        logger.error(f'failed to extract:{err}')
        return []

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
    test_file = f'{_dir}/TimeFBWorn.csv'

    data = process(test_file)
    pprint.pprint(data)
