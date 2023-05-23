"""Analyses."""
import logging


def update(garjus, projects=None):
    """Update analyses."""
    for p in (projects or garjus.projects()):
        if p in projects:
            logging.info(f'updating analyses:{p}')
            _update_project(garjus, p)


def _update_project(garjus, project):
    analyses = garjus.analyses(project, download=True)

    if len(analyses) == 0:
        logging.info(f'no analyses for project:{project}')
        return

    # Get a snapshot of project scan/assr/sgp data
    assessors = garjus.assessors(projects=[project])
    scans = garjus.scans(projects=[project])
    sgp = garjus.subject_assessors(projects=[project])
    project_data = {}
    project_data['name'] = project
    project_data['scans'] = scans
    project_data['assessors'] = assessors
    project_data['sgp'] = sgp

    # Handle each record
    for i, row in analyses.iterrows():
        aname = row['NAME']

        logging.info(f'updating analysis:{aname}')

        update_analysis(
            garjus,
            filepath,
            subjects,
            project_data)


def update_analysis(
    garjus,
    filepath,
    subjects,
    project_data):

    # Load the processor
    project_processor = load_from_yaml(garjus, filepath)

    (assr, info) = project_processor.get_assessor(
        garjus, subjects, project_data)

    # TODO: apply reproc or rerun if needed

    if info['PROCSTATUS'] in [NEED_TO_RUN, NEED_INPUTS]:
        logging.debug('building task')
        #(assr, info) = build_task(garjus, assr, info, processor, project_data)
        #logging.debug(f'assr after={info}')
    else:
        logging.debug('already built:{}'.format(info['ASSR']))


def load_from_yaml(
    xnat,
    filepath,
    job_template='~/job_template.txt',
):
    """
    Load processor from yaml
    :param filepath: path to yaml file
    :return: processor
    """

    processor = None
    proc_level = get_processor_level(filepath)

    if proc_level == 'subject':
        logging.debug('loading as SGP:{}'.format(filepath))
        processor = SgpProcessor(
            xnat,
            filepath,
            user_inputs,
            singularity_imagedir,
            job_template)
    else:
        logging.debug('loading as Processor_v3_1:{}'.format(filepath))
        processor = Processor_v3_1(
            xnat,
            filepath,
            user_inputs,
            singularity_imagedir,
            job_template)

    return processor

