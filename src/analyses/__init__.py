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

    # Get scan/assr/sgp data
    #assessors = garjus.assessors(projects=[project])
    #scans = garjus.scans(projects=[project])
    sgp = garjus.subject_assessors(projects=[project])

    project_data = {}
    project_data['name'] = project
    #project_data['scans'] = scans
    #project_data['assessors'] = assessors
    project_data['sgp'] = sgp

    # Handle each record
    for i, row in analyses.iterrows():
        aname = row['NAME']

        logging.info(f'updating analysis:{aname}')

        #build_analysis(
        #    garjus,
        #    project_data)
