# only update "newscans" after removing already uploaded from list

import os
import importlib.machinery
import glob
import logging

import redcap


def update_all(image_dir):
    # REMBRANDT
    PROJECT = 'REMBRANDT'
    XST2NST = {
        'Survey SHC32': 'Localizer scan',
        'T1': 'MR structural (T1)',
        'FLAIR': 'MR: FLAIR',
        'fMRI_REST1': 'fMRI',
        'fMRI_REST2': 'fMRI',
        'fMRI_REST_FSA': 'Field Map',
        'fMRI_MSIT': 'fMRI',
        'DTI_2min_b1000': 'MR diffusion',
        'DTI_2min_b1000apa': 'MR diffusion',
        'DTI_2min_b2000': 'MR diffusion',
        'CTAC': 'PET',
    }
    XST2NEI = {
        'fMRI_REST1': '1768',
        'fMRI_REST2': '1768',
        'fMRI_MSIT': '1769',
    }
    proj = get_redcap('104046')
    logging.info(PROJECT)
    m.process_project(
        project=proj,
        xnat_project=PROJECT,
        type_map=XST2NST,
        exp_map=XST2NEI,
        nda_template=f'{image_dir}/image03_template.csv',
        image_dir=f'{image_dir}/{PROJECT}')

    # D3
    PROJECT = 'D3'
    XST2NST = {
        'Survey SHC32': 'Localizer scan',
        'T1': 'MR structural (T1)',
        'FLAIR': 'MR: FLAIR',
        'fMRI_REST1': 'fMRI',
        'fMRI_REST2': 'fMRI',
        'fMRI_EEfRT1': 'fMRI',
        'fMRI_EEfRT2': 'fMRI',
        'fMRI_EEfRT3': 'fMRI',
        'fMRI_MIDT1': 'fMRI',
        'fMRI_MIDT2': 'fMRI',
        'fMRI_REST_FSA': 'Field Map',
        'NM': 'Neuromelanin MRI (NM-MRI)',
        'CTAC': 'PET',
    }
    XST2NEI = {
        'fMRI_REST1': '1768',
        'fMRI_REST2': '1768',
        'fMRI_EEfRT1': '1794',
        'fMRI_EEfRT2': '1794',
        'fMRI_EEfRT3': '1794',
        'fMRI_MIDT1': '1795',
        'fMRI_MIDT2': '1795',
    }
    proj = get_redcap('113422')
    logging.info(PROJECT)
    m.process_project(
        project=proj,
        xnat_project=PROJECT,
        type_map=XST2NST,
        exp_map=XST2NEI,
        nda_template=f'{image_dir}/image03_template.csv',
        image_dir=f'{image_dir}/{PROJECT}')

    # DepMIND2
    PROJECT = 'DepMIND2'
    XST2NST = {
        'Survey SHC32': 'Localizer scan',
        'T1': 'MR structural (T1)',
        'FLAIR': 'MR: FLAIR',
        'fMRI_REST1': 'fMRI',
        'fMRI_REST2': 'fMRI',
        'fMRI_EmoStroop': 'fMRI',
        'fMRI_SPT': 'fMRI',
        'ASL': 'ASL',
        'M0': 'M0'
    }
    XST2NEI = {
        'fMRI_REST1': '1768',
        'fMRI_REST2': '1768',
        'fMRI_EmoStroop': '1920',
        'fMRI_SPT': '1922',
    }
    proj = get_redcap('108807')
    logging.info(PROJECT)
    m.process_project(
        project=proj,
        xnat_project=PROJECT,
        type_map=XST2NST,
        exp_map=XST2NEI,
        nda_template=f'{image_dir}/image03_template.csv',
        image_dir=f'{image_dir}/{PROJECT}')

    print('DONE!')


def update_batches(image_dir):
    # For each .csv file in batches create a folder with the same name and copy
    # the zips specified in the csv to that new folder. If the folder already
    # exists, refuse to overwrite, require it to be manually removed.

    try:
        m = importlib.import_module('functions.update_image03')
    except ModuleNotFoundError as err:
        print(f'error loading functions.update_image03:{err}')

    csv_list = glob.glob(f'{image_dir}/batches/*.csv')

    for csvfile in csv_list:
        print(csvfile)
        m.update_imagedir(csvfile, image_dir)

    print('Done!')


if __name__ == "__main__":
    ROOTIMAGEDIR = '/Volumes/SharedData/admin-OneDrive/OneDrive - VUMC/NDA_image03'
    #ROOTIMAGEDIR = '/Users/boydb1/Downloads/NDA_image03'

    image_dir = ROOTIMAGEDIR
    update_all(image_dir)
    #update_batches(image_dir)
