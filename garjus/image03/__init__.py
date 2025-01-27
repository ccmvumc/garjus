"""Garjus NDA image03 Management."""

# DESCRIPTION:
# image03 update and download, queries REDCap and XNAT to get updated info,
# save csv download each DIDOM zip
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
import glob
import logging
from numpy import datetime_as_string

from zipfile import BadZipFile
import pandas as pd


logger = logging.getLogger('garjus.image03')

IMAGE03_TEMPLATE = "https://nda.nih.gov/api/datadictionary/v2/datastructure/image03/template"


def update(garjus, projects=None, startdate=None, enddate=None, sites=None):
    """Update image03 batches."""
    for p in (projects or garjus.projects()):
        if p in projects:
            logger.debug(f'updating image03:{p}')
            _update_project(garjus, p, startdate, enddate, sites)


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


def _update_project(garjus, project, startdate=None, enddate=None, sites=None):
    xst2nei = None

    # Get map of Xnat scan types to NDA scan types
    xst2nst = garjus.project_setting(project, 'xst2nst')
    if not xst2nst:
        logger.debug('no xst2nst')
        return

    # Check for site specific experiment types in xst2nei
    if len(sites) == 1:
        site_data = garjus.sites(project)
        for rec in site_data:
            if rec['site_shortname'] == sites[0] and rec['site_xst2nei']:
                xst2nei = rec['site_xst2nei']

    if not xst2nei:
        # Get map of Xnat scan types to NDA experiment types
        xst2nei = garjus.project_setting(project, 'xst2nei')
        if not xst2nei:
            logger.debug('no xst2nei')
            return

    # Parse strings into dictionary
    xst2nst = _parse_map(xst2nst)
    xst2nei = _parse_map(xst2nei)

    logger.debug(f'settings:{project}:xst2nei={xst2nei}:xst2nst={xst2nst}')

    outfile = f'{project}_image03.csv'

    _make_image03_csv(
        garjus,
        project,
        xst2nst,
        xst2nei,
        outfile,
        startdate,
        enddate,
        sites=sites)


def _download_dicom_zip(scan, zipfile):
    dstdir = os.path.dirname(zipfile)

    # Make the output directory
    try:
        os.makedirs(dstdir)
    except FileExistsError:
        pass

    # Download zip of resource
    if scan.resource('DICOM').exists():
        try:
            dst_zip = scan.resource('DICOM').get(dstdir, extract=False)
            return dst_zip
        except BadZipFile as err:
            logger.error(f'error downloading:{err}')
            return None
    elif scan.resource('DICOMZIP').exists():
        try:
            res = scan.resource('DICOMZIP')
            src_zip = res.files().get()[0]
            res.file(src_zip).get(zipfile)
        except BadZipFile as err:
            logger.error(f'error downloading:{err}')
            return None
    else:
        logger.error(f'error downloading, DICOM not found')
        return None


def _touch_dicom_zip(scan, zipfile):
    dstdir = os.path.dirname(zipfile)

    # Make the output directory
    try:
        os.makedirs(dstdir)
    except FileExistsError:
        pass

    with open(zipfile, 'w'):
        pass


def _mr_info(scan_info, type_map, exp_map):
    scan_type = scan_info['SCANTYPE']
    scan_date = scan_info['DATE']
    subj_label = scan_info['SUBJECT']
    scan_label = scan_info['SCANID']

    zip_path = os.path.join(
        f'{subj_label}_MR_{str(scan_date).split(" ")[0]}'.replace('-',''),
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

    if 'SEX' in scan_info:
        info['sex'] = scan_info['SEX']
    else:
        logger.debug(f'SEX not found')
        info['sex'] = ''

    if 'GUID' in scan_info:
        info['subjectkey'] = scan_info['GUID']
    else:
        logger.debug(f'GUID not found')
        info['subjectkey'] = ''

    if 'SCANAGE' in scan_info:
        info['interview_age'] = scan_info['SCANAGE']
    else:
        logger.debug(f'SCANAGE not found')
        info['interview_age'] = ''

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
        f'{subj_label}_PET_{str(scan_date).split(" ")[0]}'.replace('-',''),
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

    if 'SEX' in scan_info:
        info['sex'] = scan_info['SEX']
    else:
        logger.debug(f'SEX not found')
        info['sex'] = ''

    if 'GUID' in scan_info:
        info['subjectkey'] = scan_info['GUID']
    else:
        logger.debug(f'GUID not found')
        info['subjectkey'] = ''

    if 'SCANAGE' in scan_info:
        info['interview_age'] = scan_info['SCANAGE']
    else:
        logger.debug(f'SCANAGE not found')
        info['interview_age'] = ''

    return info


def get_image03_df(mr_scans, pet_scans, type_map, exp_map):
    data = []

    # Load the MRIs
    for cur_scan in mr_scans.to_dict('records'):
        data.append(_mr_info(cur_scan, type_map, exp_map))

    # Load the PETs
    for cur_scan in pet_scans.to_dict('records'):
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
            scan_label = f['image_file'].split('/')[1].split('_')[0]

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
            _download_dicom_zip(scan, cur_file)
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
        return df

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
    outfile,
    startdate=None,
    enddate=None,
    sites=None,
):
    dfs = garjus.subjects(project, include_dob=True)

    # Get the MRIs
    mscans = garjus.scans(
        projects=[project],
        scantypes=type_map.keys(),
        modalities=['MR'],
        sites=sites,
        startdate=startdate,
        enddate=enddate)

    # Get the PETs
    pscans = garjus.scans(
        projects=[project],
        scantypes=type_map.keys(),
        modalities=['PET'],
        sites=sites,
        startdate=startdate,
        enddate=enddate)

    # merge in subject data
    mscans = pd.merge(mscans, dfs, left_on='SUBJECT', right_on='ID')
    pscans = pd.merge(pscans, dfs, left_on='SUBJECT', right_on='ID')

    if 'DOB' in mscans:
        mscans.DOB = pd.to_datetime(mscans.DOB)
        pscans.DOB = pd.to_datetime(pscans.DOB)

        # Calculate Scan age in integer of months as a string
        mscans['SCANAGE'] = (mscans['DATE'] + pd.DateOffset(days=15)) - mscans['DOB']
        mscans['SCANAGE'] = mscans['SCANAGE'].values.astype('<m8[M]').astype('int').astype('str') 
        pscans['SCANAGE'] = (pscans['DATE'] + pd.DateOffset(days=15)) - pscans['DOB']
        pscans['SCANAGE'] = pscans['SCANAGE'].values.astype('<m8[M]').astype('int').astype('str')
    else:
        logger.debug(f'DOB not found, cannot calculate scan age')
        mscans['SCANAGE'] = ''
        pscans['SCANAGE'] = ''

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
