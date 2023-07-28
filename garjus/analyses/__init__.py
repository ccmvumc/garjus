"""Analyses."""
import logging
import os


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

    # Handle each record
    for i, row in analyses.iterrows():
        aname = row['NAME']

        logging.info(f'updating analysis:{aname}')

        update_analysis(
            garjus,
            filepath,
            subjects)


def update_analysis(
    garjus,
    project,
    analysis_id):

    print('TBD:update_analysis')


def download_analysis_inputs(garjus, project, analysis_id, download_dir):
    print(f'download_analysis_inputs:{project}:{analysis_id}:{download_dir}')

    # Make the output directory
    try:
        os.makedirs(download_dir)
    except FileExistsError:
        pass

    # Determine what we need to download
    a = garjus.load_analysis(project, analysis_id)

    print(a['processor'])

    assessors = garjus.assessors(projects=[project])

    print(assessors)

    #'inputs': {'xnat': {'subjects': 
    #{'sessions': {'types': 'Baseline',
    #'assessors': [{'name': 'assr_msit', 'types': 'fmri_msit_v2',
    #'resources': [{'resource': '1stLEVEL', 'fmatch': 'con_0002.nii.gz'},
    #{'resource': '1stLEVEL', 'fmatch': 'behavior.txt'}]}]}}}}]

    # TODO: first confirm everything is found on xnat for these subjects

    subjects = a['analysis_include'].splitlines()
    for s in subjects:
        subj_assessors = assessors[assessors.SUBJECT == s]
        print(s, subj_assessors)

        sdir = f'{download_dir}/{s}'

        try:
            os.makedirs(sdir)
        except FileExistsError:
            pass

        # get the subjects sessions

        # filter by types
        # iterate sessions
            # get the sessions assessors
            # filter by resource


    print(a['processor']['inputs']['xnat']['subjects'])


    # Download it

    # Done