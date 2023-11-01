"""NIH Examiner data extraction."""
import logging
import tempfile
import subprocess
import os

import pandas as pd


# CPT Summary File
cpt_columns = [
    'target_corr',
    'target_errors',
    'nontarget_corr',
    'nontarget_errors',
    'target_mean',
    'target_median',
    'target_stdev',
    'performance_errors']


# Flanker Summary File
flank_columns = [
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
    'flanker_error_diff']


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
    'nb2int_stdev']


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
    'shift_error_diff']


# Output columns we want to keep
scoring_columns = [
    'executive_composite',
    'executive_se',
    'fluency_factor',
    'fluency_se',
    'cog_control_factor',
    'cog_control_se',
    'working_memory_factor',
    'working_memory_se']


# Columns required by scoring program
input_columns = [
    'subject_id',
    'session_date',
    'site_id',
    'session_num',
    'language',
    'age',
    'dot_total',
    'nb1_score',
    'nb2_score',
    'flanker_score',
    'error_score',
    'antisacc',
    'shift_score',
    'vf1_corr',
    'vf2_corr',
    'cf1_corr',
    'cf2_corr']


def process(
    manual_values,
    flank_file,
    cpt_file,
    nback_file,
    shift_file,
):
    """Process NIH Examiner files and return subset of data."""
    mv = manual_values

    # Extract data from files
    flank_data = _extract_onerow_file(flank_file)
    cpt_data = _extract_onerow_file(cpt_file)
    nback_data = _extract_onerow_file(nback_file)
    shift_data = _extract_onerow_file(shift_file)

    antisacc = mv['anti_trial_1'] + mv['anti_trial_2']

    behav_total = 0
    try:
        for i in range(9):
            b = int(mv[f'brs_{i+1}'])
            if b < 4:
                behav_total += b
    except Exception as err:
        logging.error(err)
        return

    # Calculate error score
    error_score = \
        cpt_data['nontarget_errors'] + \
        flank_data['flanker_error_diff'] + \
        shift_data['shift_error_diff'] + \
        mv['vf1_rep'] + \
        mv['vf1_rv'] + \
        mv['vf2_rep'] + \
        mv['vf2_rv'] + \
        mv['cf1_rep'] + \
        mv['cf1_rv'] + \
        mv['cf2_rep'] + \
        mv['cf2_rv'] + \
        behav_total

    # Collect inputs
    inputs = {
        'subject_id': '',
        'session_date': '',
        'site_id': '',
        'session_num': '',
        'language': '1',
        'age': '',
        'dot_total': mv['dot_total'],
        'antisacc': antisacc,
        'vf1_corr': mv['vf1_corr'],
        'vf2_corr': mv['vf2_corr'],
        'cf1_corr': mv['cf1_corr'],
        'cf2_corr': mv['cf2_corr'],
        'nb1_score': nback_data['nb1_score'],
        'nb2_score': nback_data['nb2_score'],
        'flanker_score': flank_data['flanker_score'],
        'error_score': error_score,
        'shift_score': shift_data['shift_score'],
    }

    # Run the Examiner Scoring Program
    with tempfile.TemporaryDirectory() as tmpdir:
        score_data = _scoring(inputs, tmpdir)

    # Transform data for upload
    return _transform(flank_data, cpt_data, nback_data, shift_data, score_data)


def _transform(flank_data, cpt_data, nback_data, shift_data, score_data):
    """Take data extracted from files and prep for REDCap"""
    data = {}
    data.update(_subset(flank_data, flank_columns))
    data.update(_subset(cpt_data, cpt_columns))
    data.update(_subset(nback_data, nback_columns))
    data.update(_subset(shift_data, shift_columns))
    data.update(_subset(score_data, scoring_columns))
    return data


def _extract_onerow_file(filename):
    """Extract data from file that has a header row and one data row"""
    try:
        df = pd.read_csv(filename)
    except:
        df = pd.read_excel(filename)

    if len(df) > 1:
        logging.warn('multiple rows, using last!')

    # Get data from last row
    return df.iloc[-1].to_dict()


def _subset(data, columns):
    """Return subset of key/values based on column or key names specified."""
    return {k: v for k, v in data.items() if k in columns}


def _scoring(inputs, tmpdir):
    """Run the Examiner Scoring program and return outputs"""
    inputfile = f'{tmpdir}/input.csv'
    outputfile = f'{tmpdir}/output.csv'

    input_data = [str(inputs.get(x, '')) for x in input_columns]

    # Save input file
    with open(inputfile, 'w') as f:
        f.write(','.join(input_columns))
        f.write('\n')
        f.write(','.join(input_data))
        f.write('\n')
        f.write(','.join(input_data))
        f.write('\n')

    # Run the scoring program with the input file
    rwd = os.path.expanduser(
        '~/git/garjus/garjus/automations/etl_nihexaminer/Scoring')

    if not os.path.exists(rwd):
        rwd = os.path.expanduser(
            '~/garjus/garjus/automations/etl_nihexaminer/Scoring')

    script = 'examiner_scoring.R'
    cmd = f'Rscript --vanilla'
    cmd += f' -e "setwd(\'{rwd}\')"'
    cmd += f' -e "source(\'{script}\')"'
    cmd += f' -e "score_file(\'{inputfile}\', \'{outputfile}\')"'
    res = subprocess.call(cmd, shell=True)

    # Load values from output file
    return _extract_onerow_file(outputfile)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    _dir = os.path.expanduser('~/Downloads')
    flank_file = f'{_dir}/Flanker_Summary_v1187_1_07_14_2021_11h_16m.csv'
    cpt_file = f'{_dir}/CPT_Summary_v1071_1_07_08_2021_11h_12m.csv'
    nback_file = f'{_dir}/NBack_Summary_v1071_1_07_08_2021_11h_17m.csv'
    shift_file = f'{_dir}/SetShifting_Summary_v1071_1_07_08_2021_10h_59m.csv'
    data = process(mv, flank_file, cpt_file, nback_file, shift_file)
    import pprint
    pprint.pprint(data)
    pprint.pprint(data.keys())
