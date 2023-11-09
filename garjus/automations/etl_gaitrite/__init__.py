"""Gaitrite data extraction."""
import logging
import tempfile
import subprocess
import os

import pandas as pd


logger = logging.getLogger('garjus.automations.etl_gaitrite')


def process(gaitrite_file):
    """Process Gaitrite file and return subset of data."""

    data = _extract(gaitrite_file)

    return data

 
def _extract(filename):
    """Extract data from file that has a header row and one data row"""
    try:
        df = pd.read_csv(filename)
    except:
        df = pd.read_excel(filename)

    # Get data from last row
    return df.to_dict()

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
    pprint.pprint(data.keys())
