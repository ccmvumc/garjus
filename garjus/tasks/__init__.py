"""Tasks."""
import logging

from .processors import build_processor


logger = logging.getLogger('garjus.tasks')


def update(garjus, projects=None):
    """Update tasks."""
    for p in (projects or garjus.projects()):
        if p in projects:
            logger.debug(f'updating tasks:{p}')
            _update_project(garjus, p)


def _update_project(garjus, project):
    # Get protocol data, download yaml files as needed
    protocols = garjus.processing_protocols(project, download=True)

    if len(protocols) == 0:
        logger.info(f'no processing protocols for project:{project}')
        return

    # Get scan/assr/sgp data
    assessors = garjus.assessors(projects=[project])
    scans = garjus.scans(projects=[project])
    sgp = garjus.subject_assessors(projects=[project])

    project_data = {}
    project_data['name'] = project
    project_data['scans'] = scans
    project_data['assessors'] = assessors
    project_data['sgp'] = sgp

    # Iterate processing protocols
    for i, row in protocols.iterrows():
        filepath = row['FILE']

        logger.info(f'file:{filepath}')

        user_inputs = row.get('ARGS', None)
        if user_inputs:
            logger.debug(f'overrides:{user_inputs}')
            rlist = user_inputs.strip().split('\r\n')
            rdict = {}
            for arg in rlist:
                try:
                    key, val = arg.split(':', 1)
                    rdict[key] = val.strip()
                except ValueError as e:
                    msg = f'invalid arguments:{project}:{filepath}:{arg}:{e}'
                    raise Exception(msg)

            user_inputs = rdict
            logger.debug(f'user_inputs:{user_inputs}')

        if row['FILTER']:
            include_filters = str(row['FILTER']).replace(' ', '').split(',')
        else:
            include_filters = []

        build_processor(
            garjus,
            filepath,
            user_inputs,
            project_data,
            include_filters)
