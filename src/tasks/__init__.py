"""Tasks."""
import logging
import os

from .processors import build_processor


# TODO: Handle project level processing


def update(garjus, projects=None):
    """Update tasks."""
    for p in (projects or garjus.projects()):
        if p in projects:
            logging.info(f'updating tasks:{p}')
            _update_project(garjus, p)


def _update_project(garjus, project):

    # TODO: get these from project settings
    singularity_imagedir = '/data/mcr/centos7/singularity'
    processorlib = '/data/mcr/centos7/dax_processors'
    job_template = '/data/mcr/centos7/dax_templates/job_template_v3.txt'

    # Get protocol data
    protocols = garjus.processing_protocols(project)

    if len(protocols) == 0:
        logging.info(f'no processing protocols for project:{project}')
        return

    print(protocols)

    # Get scan/assr/sgp data
    assessors = garjus.assessors(projects=[project])
    scans = garjus.scans(projects=[project])
    sgp = garjus.subject_assessors(projects=[project])

    print(scans)

    project_data = {}
    project_data['name'] = project
    project_data['scans'] = scans
    project_data['assessors'] = assessors
    project_data['sgp'] = sgp

    # Iterate processing protocols
    for i, row in protocols.iterrows():

        if row['FILE'] == 'CUSTOM':
            filepath = row['CUSTOM']
        else:
            filepath = row['FILE']

        if not os.path.isabs(filepath):
            # Prepend lib location
            filepath = os.path.join(processorlib, filepath)

        if not os.path.isfile(filepath):
            logging.warn(f'invalid file path:{filepath}')
            continue

        logging.info(f'file:{filepath}')

        user_inputs = row.get('ARGS', None)
        logging.debug(f'overrides:{user_inputs}')

        if user_inputs:
            rlist = user_inputs.strip().split('\r\n')
            rdict = {}
            for arg in rlist:
                try:
                    key, val = arg.split(':', 1)
                    rdict[key] = val.strip()
                except ValueError as e:
                    msg = f'invalid arguments:{project}:{filepath}:{arg}:{e}'
                    raise XnatUtilsError(msg)

            user_inputs = rdict

        logging.debug(f'user_inputs:{user_inputs}')

        if row['FILTER']:
            include_filters = str(row['FILTER']).replace(' ', '').split(',')
        else:
            include_filters = []

        build_processor(
            garjus,
            filepath,
            singularity_imagedir,
            job_template,
            user_inputs,
            project_data,
            include_filters)
