"""

Garjus NDA image03 Management

"""
# To create new batch, run update to get updated newscans, 
# copy newscans to "PROJECT-batch_name_image03" and rename csv with same prefix.
# Remove any scans you don't want included, e.g. too new. 
# Remove from both csv and dir. Then run update.py to conform these appear 
# in newscans again (or move them there instead of deleting), 
# let .csv be recreated.

# so, run_update keeps the root folder for each project up to date with a csv file
# when we are ready to upload, we make a csv with just the rows to be uploaded
# and we run update_imagedir() which will copy zips from the root dir, the zips
# specified in the csv

# TODO: arrange folders by upload batch: jan2022,july2022,jan2023
# TODO: one function to create the csv without downloading anything new,
# leave new stuff empty
# TODO: one function to make the csv, another to take the csv and update a folder

# TODO: move the project specific stuff out of here and into the project params
# yaml files. Then make this dynamic based on those settings. Just need to know
# the redcap fields to use to find: subj_num, record_id, mri_date, and dob.
# We could provide defaults for this since most are the same, and then allow
# override via args. The settings in each of the run_ functions should
# also be moved to project yaml.


# DESCRIPTION:
# image03 update script that queries REDCap and XNAT to get updated info,
# updates dicom zips on Box, then saves csv (overwriting existing).
# The user that wants to upload to NDA just needs to download
# the current images and change the paths in the csv (alternatively, you
# could run the upload from the admin account on mrburns).
# ==========================================================================
# INPUTS:
# *project
# *start date (optional)
# *end date (optional)
# *mapping of scan types to "scan_type"
# ==========================================================================
# OUTPUTS:
# *_image03.csv
# *zip of each series organized as SESSION/SCAN/DICOM.zip
# ==========================================================================
# CSV columns are the 14 required fields (including conditionally required):
# subjectkey = REDCap-->guid
# src_subject_id = internal subject number, e.g. 14295
# interview_date = scan date
# interview_age = age at date in months-->use "dob" to calculate age
# sex = REDCap-->sex_xcount
# image_file = (path to file on Box)
# image_description = (XNAT scan type)
# scan_type = (MR diffusion, fMRI, MR structural (T1), MR: FLAIR, etc.)
# scan_object = "Live"
# image_file_format = "DICOM"
# image_modality = "MRI"
# transformation_performed = "No"
# experiment_id = (fmri only, linked to experiment in NDA)
# bvek_bval_files = "Yes" (diffusion only)
# ==========================================================================
# TODO:
# *REDCap field for "uploaded/exported to NDA=yes or no" or just use date
# *later: integrate this script with run_functions.py but run daily not hourly
# ==========================================================================

import os, shutil
import glob

from dax import XnatUtils
from zipfile import BadZipFile
import pandas as pd
import redcap


CSVFILE = '_image03.csv'


# scans(self, projects=None, scantypes=None, modalities='MR')



def download_dicom_zip(scan, zipfile):
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
        print('error downloading', str(err))
        return None


def touch_dicom_zip(scan, zipfile):
    dstdir = os.path.dirname(zipfile)

    # Make the output directory
    try:
        os.makedirs(dstdir)
    except FileExistsError:
        pass

    with open(zipfile, 'w') as f:
        pass


def get_mr_info(scan_dict, type_map, exp_map, image_dir):
    scan_type = scan_dict['xnat:imagescandata/type']
    scan_date = scan_dict['xnat:imagesessiondata/date']
    proj_label = scan_dict['project']
    subj_label = scan_dict['subject_label']
    sess_label = scan_dict['label']
    scan_label = scan_dict['xnat:imagescandata/id']

    info = {
        'scan_object': 'Live',
        'image_file_format': 'DICOM',
        'image_modality': 'MRI',
        'transformation_performed': 'No'}

    zip_path = os.path.join(
        image_dir,
        '{}_MR_{}'.format(subj_label, scan_date.replace('-', '')),
        '{}_{}'.format(scan_label, scan_type.replace(' ', '_')),
        'DICOM.zip')

    info['image_file'] = zip_path
    info['src_subject_id'] = subj_label
    info['interview_date'] = scan_date
    info['image_description'] = scan_type
    info['scan_type'] = type_map[scan_type]
    info['xnat_project_label'] = proj_label
    info['xnat_subject_label'] = subj_label
    info['xnat_session_label'] = sess_label
    info['xnat_scan_label'] = scan_label

    if scan_type.startswith('DTI'):
        info['bvek_bval_files'] = 'Yes'

    if scan_type in exp_map.keys():
        info['experiment_id'] = exp_map[scan_type]

    return info


def get_pet_info(scan_dict, type_map, image_dir):
    modality = 'PET'
    scan_type = scan_dict['xnat:imagescandata/type']
    scan_date = scan_dict['xnat:imagesessiondata/date']
    proj_label = scan_dict['project']
    subj_label = scan_dict['subject_label']
    sess_label = scan_dict['label']
    scan_label = scan_dict['xnat:imagescandata/id']

    info = {
        'scan_object': 'Live',
        'image_file_format': 'DICOM',
        'image_modality': modality,
        'transformation_performed': 'No'}

    zip_path = os.path.join(
        image_dir,
        '{}_{}_{}'.format(subj_label, modality, scan_date.replace('-', '')),
        '{}_{}'.format(scan_label, scan_type.replace(' ', '_')),
        'DICOM.zip')

    info['image_file'] = zip_path
    info['src_subject_id'] = subj_label
    info['interview_date'] = scan_date
    info['image_description'] = scan_type
    info['scan_type'] = type_map[scan_type]
    info['xnat_project_label'] = proj_label
    info['xnat_subject_label'] = subj_label
    info['xnat_session_label'] = sess_label
    info['xnat_scan_label'] = scan_label

    return info


def get_image03_df(project, type_map, exp_map, image_dir, nda_template):
    data = []

    df = pd.read_csv(nda_template, skiprows=1)

    for cur_scan in get_mr_scans(project, type_map.keys()):
        scan_info = get_mr_info(cur_scan, type_map, exp_map, image_dir)
        data.append(scan_info)

    for cur_scan in get_pet_scans(project, type_map.keys()):
        scan_info = get_pet_info(cur_scan, type_map, image_dir)
        data.append(scan_info)

    df = pd.concat([df, pd.DataFrame(data)])

    return df


def update_files(df):
    ecount = 0
    dcount = 0

    with XnatUtils.get_interface() as xnat:
        for i, f in df.iterrows():
            cur_file = f['image_file']
            if os.path.exists(cur_file):
                ecount += 1
                continue

            # download it
            print(i, 'downloading', cur_file)

            scan = xnat.select_scan(
                f['xnat_project_label'],
                f['xnat_subject_label'],
                f['xnat_session_label'],
                f['xnat_scan_label'])
            download_dicom_zip(scan, cur_file)
            #touch_dicom_zip(scan, cur_file)
            dcount += 1

    print(f'found {ecount} existing files')
    print(f'found {dcount} downloaded files')


def load_redcap_rembrandt(project):
    # Load subject records from redcap
    print('loading subject demographics')
    fields = ['guid', 'sex_xcount', 'dob', 'site', 'record_id', 'subj_num']
    rec = project.export_records(fields=fields, raw_or_label='label')
    rec = [x for x in rec if x['site'] == 'Vanderbilt']
    rec = [x for x in rec if x['subj_num']]
    dfs = pd.DataFrame(rec, columns=fields)

    # For subjects that initially were in initial treatment phase,
    # get demographics from that the ITP record
    fields = ['guid', 'sex_xcount', 'dob', 'record_id', 'subj_num']
    rec = project.export_records(fields=fields, raw_or_label='label')
    dfi = pd.DataFrame(rec, columns=fields)
    for i, row in dfs.iterrows():
        # Find subject record in ITP
        asubj_num = 'a' + row['subj_num']
        dfa = dfi[dfi['subj_num'] == asubj_num]
        if len(dfa) != 1:
            continue

        arow = dfa.iloc[0]
        if not row['dob']:
            print('missing dob, get from ITP', row['subj_num'])
            dfs['dob'][i] = arow['dob']

        if not row['sex_xcount']:
            print('missing sex, get from ITP', row['subj_num'])
            dfs['sex_xcount'][i] = arow['sex_xcount']

        if not row['guid']:
            print('missing guid, get from ITP', row['subj_num'])
            dfs['guid'][i] = arow['guid']

    # Set NDA values
    dfs['src_subject_id'] = dfs['subj_num']
    dfs['subjectkey'] = dfs['guid']
    dfs['sex'] = dfs['sex_xcount'].map({'Male': 'M', 'Female': 'F'})
    dfs = dfs.drop(columns=[
        'subj_num',
        'guid',
        'sex_xcount',
        'site'])

    # Load MRI records from REDCap
    print('loading mri data from redcap')
    fields = ['mri_date', 'record_id']
    rec = project.export_records(fields=fields, raw_or_label='label')
    rec = [x for x in rec if x['mri_date']]
    dfm = pd.DataFrame(rec, columns=fields)
    dfm = dfm.astype(str)

    dfm['interview_date'] = pd.to_datetime(dfm['mri_date'])
    dfm = dfm.drop(columns=['mri_date'])

    # Merge subject and mri data
    dfm = pd.merge(dfm, dfs, how='left', left_on='record_id', right_on='record_id')

    # Load Amyloid PET records
    print('loading amyloid pet data from redcap')
    project2 = get_redcap('130673')
    
    fields = ['participant_id', 'scan_date']
    rec = project2.export_records(fields=fields, raw_or_label='label')
    dfp = pd.DataFrame(rec, columns=fields)
    dfp = dfp.astype(str)

    dfp['interview_date'] = pd.to_datetime(dfp['scan_date'])
    dfp['src_subject_id'] = dfp['participant_id']
    dfp = dfp.drop(columns=[
        'scan_date',
        'participant_id'])

    print('loaded {} pet records'.format(len(dfp)))

    # Merge subject and scan data
    dfp = pd.merge(dfp, dfs, how='left', left_on='src_subject_id', right_on='src_subject_id')

    print('merged {} pet records'.format(len(dfp)))

    # Concat MRI and PET scans
    df = pd.concat([dfm, dfp])

    #print(df[df.src_subject_id == '14013'])
    #print(df[df.src_subject_id == '14129'])

    # Calculate age at scan as count of months, rounded up from 15
    print('calculate age in months')
    df['dob'] = pd.to_datetime(df['dob'])
    df['interview_date'] = pd.to_datetime(df['interview_date'])

    # Exclude incomplete data
    df = df.dropna()

    df['interview_age'] = (
        (df['interview_date'] + pd.DateOffset(days=15)) - df['dob']
    ).astype('<m8[M]').astype('int').astype('str')

    return df


def load_redcap_d3(project):
    # D3 redcap variable names
    #   dob = date of birth
    #   record_id = autogenerated ID
    #   sex_xcount
    #   guid = ndar unique ID
    #   subj_num = LACI internal ID

    # Load subject records from redcap
    print('loading subject demographics')
    fields = ['guid', 'sex_xcount', 'dob', 'record_id', 'subj_num']
    rec = project.export_records(fields=fields, raw_or_label='label')
    rec = [x for x in rec if x['dob']]
    dfs = pd.DataFrame(rec, columns=fields)
    dfs = dfs.astype(str)

    # Set values for NDA fields
    dfs['sex'] = dfs['sex_xcount'].map({'Male': 'M', 'Female': 'F'})
    dfs['subjectkey'] = dfs['guid']
    dfs['src_subject_id'] = dfs['subj_num']

    # Drop temporary columns
    dfs = dfs.drop(columns=['subj_num', 'guid', 'sex_xcount'])

    # Load MRI records from REDCap
    print('loading mri data from redcap')
    fields = ['mri_date', 'record_id']
    rec = project.export_records(fields=fields, raw_or_label='label')
    rec = [x for x in rec if x['mri_date']]
    dfm = pd.DataFrame(rec, columns=fields)
    dfm = dfm.astype(str)

    dfm['interview_date'] = pd.to_datetime(dfm['mri_date'])

    # Drop temporary columns
    dfm = dfm.drop(columns=['mri_date'])

    print('loaded {} mri records'.format(len(dfm)))

    # Load PET records from REDCap
    print('loading pet data from redcap')
    fields = ['record_id', 'scan_date']
    rec = project.export_records(fields=fields, raw_or_label='label')
    rec = [x for x in rec if x['scan_date']]
    dfp = pd.DataFrame(rec, columns=fields)
    dfp['modality'] = 'PET'
    dfp['interview_date'] = pd.to_datetime(dfp['scan_date'])

    # Drop temporary columns
    dfp = dfp.drop(columns=['scan_date'])

    print('loaded {} pet records'.format(len(dfp)))

    # Concat MRI and PET scans
    df = pd.concat([dfm, dfp])

    # Merge in subject data
    df = pd.merge(
        df, dfs, how='left', left_on='record_id', right_on='record_id')

    # Calculate age at scan as count of months, rounded up from 15
    print('calculate age in months')
    df['dob'] = pd.to_datetime(df['dob'])
    df['interview_date'] = pd.to_datetime(df['interview_date'])
    df['interview_age'] = (
        (df['interview_date'] + pd.DateOffset(days=15)) - df['dob']
    ).astype('<m8[M]').astype('int').astype('str')

    # Drop temporary columns
    df = df.drop(columns=['dob'])

    # Columns are subjectKey, sex, interview_age, interview_date
    return df


def load_redcap_depmind2(project):
    # DM2 redcap variable names
    #   dob = date of birth
    #   record_id = autogenerated ID
    #   sex_xcount
    #   guid = ndar unique ID
    #   subject_number = LACI internal ID

    # Load subject records from redcap
    print('loading subject demographics')
    fields = ['guid', 'sex_xcount', 'dob', 'record_id', 'subject_number']
    rec = project.export_records(fields=fields, raw_or_label='label')
    rec = [x for x in rec if x['dob']]
    dfs = pd.DataFrame(rec, columns=fields)
    dfs = dfs.astype(str)

    # Set values for NDA fields
    dfs['subjectkey'] = dfs['guid']
    dfs['sex'] = dfs['sex_xcount'].map({'Male': 'M', 'Female': 'F'})
    dfs['src_subject_id'] = dfs['subject_number']

    # Drop temporary columns
    dfs = dfs.drop(columns=[
        'guid',
        'sex_xcount',
        'subject_number'])

    # Load MRI records from REDCap
    print('loading mri data from redcap')
    fields = ['mri_date', 'record_id']
    rec = project.export_records(fields=fields, raw_or_label='label')
    rec = [x for x in rec if x['mri_date']]
    dfm = pd.DataFrame(rec, columns=fields)
    dfm = dfm.astype(str)

    dfm['interview_date'] = pd.to_datetime(dfm['mri_date'])
    dfm = dfm.drop(columns=['mri_date'])

    # Merge subect and MRI data
    df = pd.merge(
        dfm, dfs, how='left', left_on='record_id', right_on='record_id')

    # Calculate age at scan as count of months, rounded up from 15
    print('calculate age in months')
    df['dob'] = pd.to_datetime(df['dob'])
    df.interview_date = df.interview_date.astype('datetime64[ns]')
    df.dob = df.dob.astype('datetime64[ns]')
    df = df.dropna()

    df['interview_age'] = (
        (df['interview_date'] + pd.DateOffset(days=15)) - df['dob']
    ).astype('<m8[M]').astype('int').astype('str')

    # Drop temporary columns
    df = df.drop(columns=[
        'dob',
        'record_id'])

    # This should include columns subjectKey, sex, interview_age, mri_date
    return df


def same_data(filename, df):
    is_same = False

    # Load data from the file
    df2 = pd.read_csv(filename, dtype=str)

    # Compare contents
    try:
        if len(df.compare(df2)) == 0:
            is_same = True
    except ValueError:
        pass

    # Return the result
    return is_same


def not_downloaded(df, image_dir):
    zip_list = glob.glob(f'{image_dir}/*2*/*/*/DICOM.zip')
    zip_list = ['/'.join(z.rsplit('/', 4)[2:4]).upper() for z in zip_list]
    df = df[~df.image_file.str.rsplit('/', n=4).str[2:4].apply('/'.join).str.upper().isin(zip_list)]

    return df


def process_project(
    project,
    xnat_project,
    type_map,
    exp_map,
    nda_template,
    image_dir
):
    outfile = os.path.join(image_dir, 'newscans', CSVFILE)

    if xnat_project == 'REMBRANDT':
        dfr = load_redcap_rembrandt(project)
    elif xnat_project == 'D3':
        dfr = load_redcap_d3(project)
    elif xnat_project == 'DepMIND2':
        dfr = load_redcap_depmind2(project)
    else:
        print('invalid xnat project, cannot proceed', xnat_project)
        return

    # Make sure we don't have any duplicates
    dfr = dfr.drop_duplicates()

    print('loading xnat data')
    dfx = get_image03_df(
        xnat_project,
        type_map,
        exp_map,
        f'{image_dir}/newscans',
        nda_template)

    # Drop columns we will get from redcap
    dfx = dfx.drop(columns=['subjectkey', 'interview_age', 'sex'])

    # Merge xnat and redcap data to get demographics
    dfx.interview_date = dfx.interview_date.astype('datetime64[ns]')
    dfr.interview_date = dfr.interview_date.astype('datetime64[ns]')
    df = pd.merge(
        dfx,
        dfr,
        left_on=['src_subject_id', 'interview_date'],
        right_on=['src_subject_id', 'interview_date'])

    # Remove already downloaded
    df = not_downloaded(df, image_dir)

    # Update DICOM zips
    update_files(df)

    # Drop temporary columns
    df = df.drop(columns=[
        'xnat_project_label',
        'xnat_subject_label',
        'xnat_session_label',
        'xnat_scan_label'])

    # Set columns to same list and order as NDA template
    df = df[pd.read_csv(nda_template, skiprows=1).columns]

    # Compare to existing csv and only write to new file if something changed
    if not os.path.exists(outfile) or not same_data(outfile, df):
        # Save data to file
        print('saving to csv file')
        check_dir(os.path.dirname(outfile))
        df.to_csv(outfile, index=False)
    else:
        print('no new data, not saving csv')


def check_dir(dir_path):
    print('check_dir', dir_path)
    try:
        os.makedirs(dir_path)
    except OSError:
        if not os.path.isdir(dir_path):
            raise


def update_imagedir(csvfile, root_dir):
    df = pd.read_csv(csvfile, dtype=str)

    # Output directory where files are copied uses base name of csv file
    batch_dir = os.path.join(root_dir, os.path.splitext(os.path.basename(csvfile))[0])

    if os.path.exists(batch_dir):
        print('dir exists')
        return

    # Update DICOM zips
    ccount = 0
    mcount = 0

    for i, f in df.iterrows():
        cur_file = f['image_file']

        # Get the path to the zip in the root dir and destionation path
        _p = cur_file.rsplit('/', 4)
        _proj = _p[1].split('_')[0]
        src_file = f'{root_dir}/allscans/{_proj}/{_p[2]}/{_p[3]}/{_p[4]}'
        dst_file = f'{batch_dir}/{_p[2]}/{_p[3]}/{_p[4]}'

        if os.path.exists(dst_file):
            print('path exists:', dst_file)
            continue

        if not os.path.exists(src_file):
            print('not found in root dir', src_file)
            mcount += 1
            continue

        # move it
        print(i, 'moving', src_file, dst_file)
        check_dir(os.path.dirname(dst_file))
        #shutil.copyfile(src_file, dst_file)
        shutil.move(src_file, dst_file)
        df.iloc[i]['image_file'] = dst_file
        ccount += 1

    print(f'found {ccount} moved files')
    print(f'found {mcount} missing files')

    # Save data to file
    print('saving to csv file')
    outfile = f'{batch_dir}/_image03.csv'
    check_dir(os.path.dirname(outfile))
    df.to_csv(outfile, index=False)
