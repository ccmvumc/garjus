"""NIH Examiner data extraction."""
import logging
import tempfile

import pandas as pd


# TODO: step2 after ETL from files or pull these values too?
# Examiner Scoring to get calculated fields across domains,
# first must create input file with fields:
# language
# dot_total
# nb1_score
# nb2_score
# flanker_score
# error_score
# antisacc
# shift_score
# vf1_corr
# vf2_corr
# cf1_corr
# cf2_corr

#import subprocess
#res = subprocess.call("Rscript script.R", shell=True)

# CPT Summary File
cpt_columns = [
'target_corr',
'target_errors',
'nontarget_corr',
'nontarget_errors',
'target_mean',
'target_median',
'target_stdev',
'performance_errors',
]

# Flanker Summary File
flanker_columns = [
'congr_corr',
'congr_mean',
'congr_median',
'congr_stdev',
'incongr_corr',
'incongr_mean',
'incongr_median',
'incongr_stdev',
'total_corr',
'total_mean',
'total_median',
'total_stdev',
'flanker_score',
'flanker_error_diff',
]

# N-back Summary File
nback_columns = [
'nb1_score',
'nb1_bias',
'nb1_corr',
'nb1_errors',
'nb1_mean',
'nb1_median',
'nb1_stdev',
'nb2_score',
'nb2_bias',
'nb2_corr',
'nb2_errors',
'nb2_mean',
'nb2_median',
'nb2_stdev',
'nb2int_corr',
'nb2int_errors',
'nb2int_mean',
'nb2int_median',
'nb2int_stdev',
]

# Set-shifting Summary File
shift_columns = [
'color_corr',
'color_errors',
'color_mean',
'color_median',
'color_stdev',
'shape_corr',
'shape_errors',
'shape_mean',
'shape_median',
'shape_stdev',
'shift_corr',
'shift_errors',
'shift_mean',
'shift_median',
'shift_stdev',
'shift_score',
'shift_error_diff',
]

def process(flanker_file, cpt_file, nback_file, shift_file):
    """Process NIH Examiner files and return subset of data."""
    data = None

    # Extract data from files
    flanker_data = _extract_flanker(flanker_file)
    cpt_data = _extract_cpt(cpt_file)
    nback_data = _extract_nback(nback_file)
    shift_data = _extract_shift(shift_file)

    # Transform data for upload
    data = _transform(flanker_data, cpt_data, nback_data, shift_data)

    return data


def _transform(flanker_data, cpt_data, nback_data, shift_data):
    """Take data extracted from files and prep for REDCap"""
    data = {}
    data.update(flanker_data)
    data.update(cpt_data)
    data.update(nback_data)
    data.update(shift_data)

    return data


def _extract_onerow_file(filename, columns=None):
    data = {}

    try:
        df = pd.read_csv(filename)
    except:
        df = pd.read_excel(filename)

    if len(df) > 1:
        logging.error('multiple rows!')
        return

    # Get data from last row
    data = df.iloc[-1].to_dict()

    if columns:
        # Get subset of columns as specified
        data = {k: v for k, v in data.items() if k in columns}

    return data


def _extract_flanker(filename):
    return _extract_onerow_file(filename, columns=flanker_columns)


def _extract_cpt(filename):
    return _extract_onerow_file(filename, columns=cpt_columns)


def _extract_nback(filename):
    return _extract_onerow_file(filename, columns=nback_columns)


def _extract_shift(filename):
    return _extract_onerow_file(filename, columns=shift_columns)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    import os

    _dir = os.path.expanduser('~/Downloads')
    flanker_file = f'{_dir}/Flanker_Summary_v1187_1_07_14_2021_11h_16m.csv'
    cpt_file = f'{_dir}/CPT_Summary_v1071_1_07_08_2021_11h_12m.csv'
    nback_file = f'{_dir}/NBack_Summary_v1071_1_07_08_2021_11h_17m.csv'
    shift_file = f'{_dir}/SetShifting_Summary_v1071_1_07_08_2021_10h_59m.csv'
    data = process(flanker_file, cpt_file, nback_file, shift_file)
    import pprint
    pprint.pprint(data)
    pprint.pprint(data.keys())
