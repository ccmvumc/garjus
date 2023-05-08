import pathlib
import tempfile
import subprocess as sb
import os
import glob
import json
import io
import logging
import requests
from zipfile import ZipFile

# TODO: use path in mri_importpath field to find a zip file of the scan then 
# run import_dicom_zip(), where mri_importpath is a storage location that can 
# be for example a local Folder that is synced by Box or OneDrive, i.e.
# used as an INBOX for other sites

# TODO: compare the date of each scan to the existing session

# inputs:
# -source to be uploaded: either a path to zip, directory, gtudy URL, or \
# XNAT urlproject/subject/session

# steps:
# -unzip if needed
# -run dcm2niix to resort into one folder per series/scan
# -run dcm2niix on each series/scan
# -zip the scan (series) folder
# -upload the zip as DICOMZIP
# -upload the NIFTI, JSON, BVAL/BVEC
# -set date, tracer, scan type, description


def import_dicom(garjus, src, dst):
    logging.info(f'uploading from:{src}')

    (proj, subj, sess) =  dst.split('/')
    logging.info(f'uploading to:{proj},{subj},{sess}')

    if src.endswith('.zip'):
        import_dicom_zip(garjus, src, proj, subj, sess)
    elif src.startswith('http'):
        # e.g. gstudy link
        import_dicom_url(garjus, src, proj, subj, sess)
    elif os.path.isdir(src):
        import_dicom_dir(garjus, src, proj, subj, sess)
    else:
        logging.error(f'unsupported source specified:{src}')
        return

    logging.info('Please Note! only DICOM that successfullly converts\
        to NIFTI is uploaded as DICOMZIP')


def import_dicom_zip(garjus, zip_path, project, subject, session):
    """Import zip of directory of DICOM files, sorted or not."""
    with tempfile.TemporaryDirectory() as temp_dir:
        unzipped_dir = pathlib.Path(f'{temp_dir}/UNZIPPED')
        unzipped_dir.mkdir()
        dicom_dir = pathlib.Path(f'{temp_dir}/DICOM')
        dicom_dir.mkdir()

        # Unzip the zip to the temp folder
        logging.info(f'unzip {zip_path} to {unzipped_dir}')
        sb.run(['unzip', '-q', zip_path, '-d', unzipped_dir])

        # Rename/sort dicom
        logging.info(f'rename/sort dicom from {unzipped_dir} to {dicom_dir}')
        garjus.rename_dicom(unzipped_dir, dicom_dir)

        # Upload
        logging.info(f'uploading:{dicom_dir}')
        garjus.upload_session(dicom_dir, project, subject, session)


def import_dicom_dir(garjus, dir_path, project, subject, session):
    """Import directory of DICOM files, sorted or not."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dicom_dir = pathlib.Path(f'{temp_dir}/DICOM')
        dicom_dir.mkdir()

        # Rename/sort dicom
        logging.info(f'rename/sort dicom from {dir_path} to {dicom_dir}')
        garjus.rename_dicom(dir_path, dicom_dir)

        # Upload
        logging.info(f'uploading:{dicom_dir}')
        garjus.upload_session(dicom_dir, project, subject, session)


def import_dicom_url(garjus, url_path, project, subject, session):
    """Import url of DICOM files, sorted or not."""
    with tempfile.TemporaryDirectory() as temp_dir:

        dicom_dir = pathlib.Path(f'{temp_dir}/DICOM')
        dicom_dir.mkdir()
        unzipped_dir = pathlib.Path(f'{temp_dir}/UNZIPPED')
        unzipped_dir.mkdir()

        r = requests.get(url_path, verify=False)
        z = ZipFile(io.BytesIO(r.content))
        z.extractall(unzipped_dir)

        # Rename/sort dicom
        logging.info(f'rename/sort dicom from {unzipped_dir} to {dicom_dir}')
        garjus.rename_dicom(unzipped_dir, dicom_dir)

        # Upload
        logging.info(f'uploading:{dicom_dir}')
        garjus.upload_session(dicom_dir, project, subject, session)
