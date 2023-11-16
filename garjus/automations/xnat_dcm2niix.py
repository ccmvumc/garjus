"""dcm2niix scans in XNAT."""
import logging
import subprocess as sb
import tempfile
import os
import pathlib
import glob

from .. import utils_dcm2nii
from .. import utils_xnat


logger = logging.getLogger('garjus.automations.xnat_dcm2niix')


def process_project(
    garjus,
    project,
):
    """xnat dcm2niix."""
    results = []

    logger.debug(f'loading data:{project}')
    df = garjus.scans(projects=[project])

    # Check each scan
    for i, scan in df.iterrows():
        full_path = scan['full_path']

        if scan['QUALITY'] == 'unusable':
            logger.debug(f'skipping unusable:{scan.SESSION}:{scan.SCANID}')
            continue

        if 'NIFTI' in scan['RESOURCES']:
            logger.debug(
                f'NIFTI exists:{project}:{scan.SESSION}:{scan.SCANID}')
            continue

        if 'JSON' in scan['RESOURCES']:
            logger.debug(f'JSON exists:{project}:{scan.SESSION}:{scan.SCANID}')
            continue

        if 'DICOMZIP' in scan['RESOURCES']:
            logger.debug(f'DICOMZIP to NIFTI:{full_path}')
            try:
                _dicomzip2nifti(garjus, full_path)
            except Exception as err:
                logger.error(err)
                continue
        elif 'DICOM' in scan['RESOURCES']:
            logger.debug(f'No DICOMZIP found, using DICOM:{full_path}')
            try:
                _dicomdir2nifti(garjus, full_path)
            except Exception as err:
                logger.error(err)
                continue
        else:
            logger.debug(f'no DICOMZIP or DICOM:{full_path}')
            continue

        results.append({
            'result': 'COMPLETE',
            'description': 'dcm2niix',
            'subject': scan['SUBJECT'],
            'session': scan['SESSION'],
            'scan': scan['SCANID']})

    return results


def _dicomzip2nifti(garjus, full_path):
    res = garjus.xnat().select(f'{full_path}/resources/DICOMZIP')

    files = res.files().get()

    if len(files) == 0:
        msg = f'no DICOMZIP files found:{full_path}'
        logger.info(msg)
        raise Exception(msg)
    elif len(files) > 1:
        msg = f'too many DICOMZIP files found:{full_path}'
        raise Exception(msg)

    src = files[0]

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, src)
        res.file(src).get(zip_path)

        # unzip it
        unzipped_dir = pathlib.Path(f'{tmpdir}/UNZIPPED')
        unzipped_dir.mkdir()

        # Unzip the zip to the temp folder
        logger.debug(f'unzip {zip_path} to {unzipped_dir}')
        sb.run(['unzip', '-q', zip_path, '-d', unzipped_dir])

        # convert to NIFTI
        logger.info(f'convert to NIFTI:{full_path}')
        _d2n(unzipped_dir, res.parent())


def _dicomdir2nifti(garjus, full_path):
    res = garjus.xnat().select(f'{full_path}/resources/DICOM')

    files = res.files().get()

    if len(files) == 0:
        msg = f'no DICOM files found:{full_path}'
        logger.info(msg)
        raise Exception(msg)

    with tempfile.TemporaryDirectory() as tmpdir:
        res.get(tmpdir, extract=True)

        # convert to NIFTI
        logger.info(f'convert to NIFTI:{full_path}')
        _d2n(os.path.join(tmpdir, 'DICOM'), res.parent())


def _d2n(dicomdir, scan_object):
    nifti_list = []
    bval_path = ''
    bvec_path = ''
    json_path = ''

    # check that it hasn't been converted yet
    nifti_count = len(glob.glob(os.path.join(dicomdir, '*.nii.gz')))
    if nifti_count > 0:
        logger.info(f'nifti exists:{dicomdir}')
        return None

    # convert
    niftis = utils_dcm2nii.dicom2nifti(dicomdir)
    if not niftis:
        logger.info(f'nothing converted:{dicomdir}')
        scan_object.attrs.set('quality': 'unusable')
        return None

    # upload the converted files, NIFTI/JSON/BVAL/BVEC
    for fpath in glob.glob(os.path.join(dicomdir, '*')):
        if not os.path.isfile(fpath):
            continue

        if fpath.lower().endswith('.bval'):
            bval_path = utils_dcm2nii.sanitize_filename(fpath)
        elif fpath.lower().endswith('.bvec'):
            bvec_path = utils_dcm2nii.sanitize_filename(fpath)
        elif fpath.lower().endswith('.nii.gz'):
            nifti_list.append(utils_dcm2nii.sanitize_filename(fpath))
        elif fpath.lower().endswith('.json'):
            json_path = utils_dcm2nii.sanitize_filename(fpath)
        else:
            pass

    # More than one NIFTI
    if len(nifti_list) > 1:
        logger.warning('dcm2niix:multiple NIFTI')

    # Upload the NIFTIs
    logger.debug(f'uploading NIFTI:{nifti_list}')
    utils_xnat.upload_files(nifti_list, scan_object.resource('NIFTI'))

    if os.path.isfile(bval_path) and os.path.isfile(bvec_path):
        logger.debug('uploading BVAL/BVEC')
        utils_xnat.upload_file(bval_path, scan_object.resource('BVAL'))
        utils_xnat.upload_file(bvec_path, scan_object.resource('BVEC'))

    if os.path.isfile(json_path):
        logger.debug(f'uploading JSON:{json_path}')
        utils_xnat.upload_file(json_path, scan_object.resource('JSON'))
