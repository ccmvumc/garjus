"""Garjus NDA image03 Management."""

# DESCRIPTION:
# image03 update script that queries REDCap and XNAT to get updated info,
# updates dicom zips, saves csv
# ==========================================================================
# INPUTS:
# *project
# *start date (optional)
# *end date (optional)
# *mapping of scan types to "scan_type"
# ==========================================================================
# OUTPUTS:
# _image03.csv
# zip of each series organized as SESSION/SCAN/DICOM.zip
# ==========================================================================
# CSV columns are the 14 required fields (including conditionally required):
# subjectkey-->guid
# src_subject_id = internal subject number, e.g. 14295
# interview_date = scan date MM/DD/YYYY (NDA requires this format)
# interview_age = age at date in months-->use "dob" to calculate age
# sex
# image_file = (path to file on Box)
# image_description = (scan type)
# scan_type = (MR diffusion, fMRI, MR structural (T1), MR: FLAIR, etc.)
# scan_object = "Live"
# image_file_format = "DICOM"
# image_modality = "MRI"
# transformation_performed = "No"
# experiment_id = (fmri only, linked to experiment in NDA)
# bvek_bval_files = "Yes" (diffusion only)
# ==========================================================================

import os
import shutil
import glob
import logging
import datetime
from numpy import datetime_as_string

from dax import XnatUtils
from zipfile import BadZipFile
import pandas as pd
import redcap


logger = logging.getLogger('garjus.image03')

IMAGE03_TEMPLATE = "https://nda.nih.gov/api/datadictionary/v2/datastructure/image03/template"


def update(garjus, projects=None):
    """Update image03 batches."""
    for p in (projects or garjus.projects()):
        if p in projects:
            logger.debug(f'updating image03:{p}')
            _update_project(garjus, p)


def download(garjus, project, image03_csv, download_dir):
    update_imagedir(garjus, project, image03_csv, download_dir)


def _parse_map(mapstring):
    """Parse map stored as string into dictionary."""

    parsed_map = mapstring.replace('=', ':')

    # Parse multiline string of delimited key value pairs into dictionary
    parsed_map = dict(x.strip().split(':', 1) for x in parsed_map.split('\n'))

    # Remove extra whitespace from keys and values
    parsed_map = {k.strip(): v.strip() for k, v in parsed_map.items()}

    return parsed_map


def _update_project(garjus, project):

    #image_dir = garjus.project_setting(project, 'imagedir')
    #if not image_dir:
    #    logger.debug(f'no imagedir set for project:{project}')
    #    image_dir = ''

    xst2nst = garjus.project_setting(project, 'xst2nst')
    if not xst2nst:
        logger.debug('no xst2nst')
        return

    xst2nei = garjus.project_setting(project, 'xst2nei')
    if not xst2nei:
        logger.debug('no xst2nei')
        return

    logger.debug(f'settings:{project}:xst2nei={xst2nei}:xst2nst={xst2nst}')

    # Parse strings into dictionary
    xst2nst = _parse_map(xst2nst)
    xst2nei = _parse_map(xst2nei)

    outfile = f'{project}_image03.csv'

    _make_image03_csv(
        garjus,
        project,
        xst2nst,
        xst2nei,
        outfile)

def _download_dicom_zip(scan, zipfile):
    dstdir = os.path.dirname(zipfile)

    # Make the output directory
    try:
        os.makedirs(dstdir)
    except FileExistsError:
        pass

    # Download zip of resource
    res = scan.resource('DICOM')
    try:
        dst_zip = res.get(dstdir, extract=False)
        return dst_zip
    except BadZipFile as err:
        logger.error(f'error downloading:{err}')
        return None


def _touch_dicom_zip(scan, zipfile):
    dstdir = os.path.dirname(zipfile)

    # Make the output directory
    try:
        os.makedirs(dstdir)
    except FileExistsError:
        pass

    with open(zipfile, 'w') as f:
        pass


def _mr_info(scan_info, type_map, exp_map):
    scan_type = scan_info['SCANTYPE']
    scan_date = scan_info['DATE']
    subj_label = scan_info['SUBJECT']
    scan_label = scan_info['SCANID']

    zip_path = os.path.join(
        f'{subj_label}_MR_{datetime_as_string(scan_date, unit="D")}'.replace('-',''),
        '{}_{}'.format(scan_label, scan_type.replace(' ', '_')),
        'DICOM.zip')

    info = {
        'scan_object': 'Live',
        'image_file_format': 'DICOM',
        'image_modality': 'MRI',
        'transformation_performed': 'No'}
    info['image_file'] = zip_path
    info['src_subject_id'] = subj_label
    info['interview_date'] = scan_date
    info['image_description'] = scan_type
    info['scan_type'] = type_map[scan_type]
    info['sex'] = scan_info['SEX']
    info['subjectkey'] = scan_info['GUID']
    info['interview_age'] =  scan_info['SCANAGE']

    if scan_type.startswith('DTI'):
        info['bvek_bval_files'] = 'Yes'

    if scan_type in exp_map.keys():
        info['experiment_id'] = exp_map[scan_type]

    return info


def _pet_info(scan_info, type_map):
    scan_type = scan_info['SCANTYPE']
    scan_date = scan_info['DATE']
    subj_label = scan_info['SUBJECT']
    scan_label = scan_info['SCANID']
    zip_path = os.path.join(
        f'{subj_label}_PET_{datetime_as_string(scan_date, unit="D")}'.replace('-',''),
        '{}_{}'.format(scan_label, scan_type.replace(' ', '_')),
        'DICOM.zip')

    info = {
        'scan_object': 'Live',
        'image_file_format': 'DICOM',
        'image_modality': 'PET',
        'transformation_performed': 'No'}

    info['image_file'] = zip_path
    info['src_subject_id'] = subj_label
    info['interview_date'] = scan_date
    info['image_description'] = scan_type
    info['scan_type'] = type_map[scan_type]
    info['sex'] = scan_info['SEX']
    info['subjectkey'] = scan_info['GUID']
    info['interview_age'] =  scan_info['SCANAGE']

    return info


def get_image03_df(mr_scans, pet_scans, type_map, exp_map):
    data = []

    # Load the MRIs
    for cur_scan in mr_scans.to_records():
        data.append(_mr_info(cur_scan, type_map, exp_map))

    # Load the PETs
    for cur_scan in pet_scans.to_records():
        data.append(_pet_info(cur_scan, type_map))

    # Initialize with template columns, ignoring first row
    logger.debug('load template from web')
    df = pd.read_csv(IMAGE03_TEMPLATE, skiprows=1)

    # Append our records
    df = pd.concat([df, pd.DataFrame(data)])

    return df


def update_files(garjus, project, df, download_dir):
    ecount = 0
    dcount = 0

    # Merge in xnat info
    scans = garjus.scans(projects=[project])
    sessions = scans[['SUBJECT', 'SESSION', 'DATE']].drop_duplicates()
    sessions['interview_date'] = pd.to_datetime(sessions['DATE']).dt.strftime('%m/%d/%Y')

    df = pd.merge(
        df, 
        sessions,
        how='left',
        left_on=['src_subject_id', 'interview_date'],
        right_on=['SUBJECT', 'interview_date'])

    with garjus.xnat() as xnat:
        for i, f in df.iterrows():
            # Determine scan label
            scan_label =  f['image_file'].split('/')[1].split('_')[0]

            # Local file path
            cur_file = os.path.join(download_dir, f['image_file'])
            if os.path.exists(cur_file):
                ecount += 1
                continue

            # connect to scan
            scan = xnat.select_scan(
                project,
                f['src_subject_id'],
                f['SESSION'],
                scan_label)

            # get the file
            logger.info(f'downloading:{cur_file}')
            #_download_dicom_zip(scan, cur_file)
            _touch_dicom_zip(scan, cur_file)
            dcount += 1

    logger.info(f'{ecount} existing files, {dcount} downloaded files')


def same_data(filename, df):
    is_same = False

    # Load data from the file
    df2 = pd.read_csv(filename, dtype=str, skiprows=1)

    # Compare contents
    try:
        if len(df.compare(df2)) == 0:
            is_same = True
    except ValueError:
        pass

    # Return the result
    logger.info(f'is_same={is_same}')
    return is_same


def not_downloaded(df, image_dir):
    if not os.path.isdir(image_dir):
        logger.error(f'image directory not found:{image_dir}')
        raise FileExistsError(image_dir)

    # Get list of DICOM zips already existing
    zip_list = glob.glob(f'{image_dir}/*/*/*/DICOM.zip')

    # Standardize naming
    zip_list = ['/'.join(z.rsplit('/', 4)[2:4]).upper() for z in zip_list]

    # Now only include not downloaded
    df = df[~df.image_file.str.rsplit('/', n=4).str[2:4].apply('/'.join).str.upper().isin(zip_list)]

    return df


def _make_image03_csv(
    garjus,
    project,
    type_map,
    exp_map,
    outfile
):
    dfs = garjus.subjects(project, include_dob=True)

    # Get the MRIs
    mscans = garjus.scans(
        projects=[project],
        scantypes=type_map.keys(),
        modalities=['MR'],
        sites=['VUMC'])

    # merge in subject data
    mscans = pd.merge(mscans, dfs, left_on='SUBJECT', right_index=True)
    mscans['DATE'] = pd.to_datetime(mscans['DATE'])
    mscans['SCANAGE'] = (mscans['DATE'] + pd.DateOffset(days=15)) - mscans['DOB']
    mscans['SCANAGE'] = mscans['SCANAGE'].values.astype('<m8[M]').astype('int').astype('str')

    # Get the PETs
    pscans = garjus.scans(
        projects=[project],
        scantypes=type_map.keys(),
        modalities=['PET'],
        sites=['VUMC'])

    # merge in subject data
    pscans = pd.merge(pscans, dfs, left_on='SUBJECT', right_index=True)
    pscans['DATE'] = pd.to_datetime(pscans['DATE'])
    #df['SCANAGE'] = ((df['DATE'] + pd.DateOffset(days=15)) - df['DOB']
    #).astype('<m8[M]'
    #).astype('int').astype('str')
    pscans['SCANAGE'] = (pscans['DATE'] + pd.DateOffset(days=15)) - pscans['DOB']
    pscans['SCANAGE'] = pscans['SCANAGE'].values.astype('<m8[M]').astype('int').astype('str')

    # get the image03 formatted
    df = get_image03_df(
        mscans,
        pscans,
        type_map,
        exp_map)

    # Set columns to same list and order as NDA template
    df = df[pd.read_csv(IMAGE03_TEMPLATE, skiprows=1).columns]

    # Compare to existing csv and only write to new file if something changed
    if not os.path.exists(outfile) or not same_data(outfile, df):
        # Save data to file
        logger.info(f'saving to csv file:{outfile}')

        # write the header
        with open(outfile, 'w') as fp:
            fp.write('"image","03"\n')

        # write the rest
        df.to_csv(outfile, mode='a', index=False, date_format='%m/%d/%Y')
    else:
        logger.info(f'no new data, use existing csv:{outfile}')


def make_dirs(dir_path):
    logger.debug(f'make_dirs{dir_path}')
    try:
        os.makedirs(dir_path)
    except OSError:
        if not os.path.isdir(dir_path):
            raise


def update_imagedir(garjus, project, csvfile, download_dir):

    df = pd.read_csv(csvfile, dtype=str, skiprows=1)

    # Remove already downloaded
    df = not_downloaded(df, download_dir)

    # Update DICOM zips
    update_files(garjus, project, df, download_dir)
