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

        if 'NIFTI' in scan['RESOURCES']:
            logger.debug(f'NIFTI exists:{project}:{scan.SESSION}:{scan.SCANID}')
            continue

        if 'JSON' in scan['RESOURCES']:
            logger.debug(f'JSON exists:{project}:{scan.SESSION}:{scan.SCANID}')
            continue

        if 'DICOMZIP' not in scan['RESOURCES']:
            logger.debug(f'no DICOMZIP:{full_path}')
            continue

        logger.info(f'convert DICOMZIP to NIFTI:{full_path}')

        res = garjus.xnat().select(f'{full_path}/resources/DICOMZIP')

        files = res.files().get()

        if len(files) == 0:
            print(i, 'no DICOMZIP files found', full_path)
            continue
        elif len(files) > 1:
            print(i, 'too many DICOMZIP files found', full_path)
            continue

        src = files[0]

        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, src)
            res.file(src).get(zip_path)

            # unzip it
            unzipped_dir = pathlib.Path(f'{tmpdir}/UNZIPPED')
            unzipped_dir.mkdir()

            # Unzip the zip to the temp folder
            logger.info(f'unzip {zip_path} to {unzipped_dir}')
            sb.run(['unzip', '-q', zip_path, '-d', unzipped_dir])

            # convert to NIFTI
            _d2n(unzipped_dir, res.parent())

            results.append({
                'result': 'COMPLETE',
                'description': 'dcm2niix',
                'subject': scan['SUBJECT'],
                'session': scan['SESSION'],
                'scan': scan['SCANID']})

    return results

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
    logger.info(f'uploading NIFTI:{nifti_list}')
    utils_xnat.upload_files(nifti_list, scan_object.resource('NIFTI'))

    if os.path.isfile(bval_path) and os.path.isfile(bvec_path):
        logger.info('uploading BVAL/BVEC')
        utils_xnat.upload_file(bval_path, scan_object.resource('BVAL'))
        utils_xnat.upload_file(bvec_path, scan_object.resource('BVEC'))

    if os.path.isfile(json_path):
        logger.info(f'uploading JSON:{json_path}')
        utils_xnat.upload_file(json_path, scan_object.resource('JSON'))
