"""Wrapper functions for dcm2nii"""
import logging
import subprocess as sb
import os


def dicom2nifti(dicomdir):
    """ convert dicom to nifti + json using dcm2niix """
    cmd = f'dcm2niix -9 -ba n -z o -f %s_%d {dicomdir}'
    logging.debug(f'running cmd:{cmd}')
    try:
        sb.run(cmd, shell=True)
    except sb.CalledProcessError:
        return []

    niftis = [f for f in os.listdir(dicomdir) if f.endswith('.nii.gz')]

    return niftis


def rename_dicom(in_dir, out_dir):
    # Use "dcm2niix -r" to sort the dicoms into series folders
    # with each file named with instance number zero-padded to 5 digits
    logging.debug('creating renamed/sorted dicom...')
    try:
        sb.run([
            'dcm2niix',
            '-r',
            'y',
            '-f',
            '%s/%5r.dcm',
            '-o',
            out_dir,
            in_dir,
        ])
    except sb.CalledProcessError as err:
        logging.error(f'error:{err}')


def sanitize_filename(filename):
    _dir = os.path.dirname(filename)
    _old = os.path.basename(filename)
    _new = "".join([x if (x.isalnum() or x == '.') else "_" for x in _old])
    if _old != _new:
        # Rename with the sanitized filename
        os.rename(os.path.join(_dir, _old), os.path.join(_dir, _new))
        filename = os.path.join(_dir, _new)

    return filename
