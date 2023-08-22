"""Analyses."""
import logging
import os

import pandas as pd


logger = logging.getLogger('garjus.analyses')


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
            project,
            row['ID'])


def update_analysis(
    garjus,
    project,
    analysis_id
):
    """Update analysis."""
    print('TBD:update_analysis')


def _sessions_from_scans(scans):
    return scans[[
        'PROJECT',
        'SUBJECT',
        'SESSION',
        'SESSTYPE',
        'DATE',
        'SITE'
    ]].drop_duplicates()


def _sessions_from_assessors(assessors):
    return assessors[[
        'PROJECT',
        'SUBJECT',
        'SESSION',
        'SESSTYPE',
        'DATE',
        'SITE'
    ]].drop_duplicates()


def _download_file(garjus, proj, subj, sess, assr, res, fmatch, dst):
    # Make the folders for this file path
    print(dst)
    _make_dirs(os.path.dirname(dst))

    # Connect to the resource on xnat
    res = garjus.xnat().select_assessor_resource(proj, subj, sess, assr, res)

    # TODO: apply regex or wildcards in fmatch
    # res_obj.files()[0].label()).get(fpath)
    # res.files().label()

    res.file(fmatch).get(dst)
    return dst


def _download_resource(garjus, proj, subj, sess, assr, res, dst):
    # Make the folders for destination path
    _make_dirs(dst)

    # Connect to the resource on xnat
    res = garjus.xnat().select_assessor_resource(proj, subj, sess, assr, res)

    # Download extracted
    res.get(dst, extract=True)

    return dst


def _make_dirs(dirname):
    try:
        os.makedirs(dirname)
    except FileExistsError:
        pass


def _download_subject(garjus, subj_dir, subj_spec, proj, subj, sessions, assessors):

    # TODO: handle any subject-level assessors, sgp

    # Download the subjects sessions
    sess_spec = subj_spec['sessions']
    sess_types = sess_spec['types'].split(',')

    for i, s in sessions[sessions.SUBJECT == subj].iterrows():
        sess = s.SESSION

        # Apply session type filter
        if s.SESSTYPE not in sess_types:
            logger.debug(f'skip session, no match on type={sess}:{s.SESSTYPE}')
            continue

        sess_dir = f'{subj_dir}/{sess}'
        logger.debug(f'download_session={sess_dir}')
        _download_session(
            garjus, sess_dir, sess_spec, proj, subj, sess, assessors)


def _download_session(garjus, sess_dir, sess_spec, proj, subj, sess, assessors):
    # get the assessors for this session
    sess_assessors = assessors[assessors.SESSION == sess]

    for k, a in sess_assessors.iterrows():
        assr = a.ASSR

        for assr_spec in sess_spec['assessors']:
            logger.debug(f'assr_spec={assr_spec}')

            assr_types = assr_spec['types'].split(',')

            logger.debug(f'assr_types={assr_types}')

            if a.PROCTYPE not in assr_types:
                logger.debug(f'skip assr, no match on type={assr}:{a.PROCTYPE}')
                continue

            for res_spec in assr_spec['resources']:

                try:
                    res = res_spec['resource']
                except (KeyError, ValueError) as err:
                    logger.error(f'reading resource:{err}')
                    continue

                if 'fmatch' in res_spec:
                    # Download files
                    for fmatch in res_spec['fmatch'].split(','):

                        # Where shall we save it?
                        dst = f'{sess_dir}/{assr}/{res}/{fmatch}'

                        # Have we already downloaded it?
                        if os.path.exists(dst):
                            logger.debug(f'exists:{dst}')
                            continue

                        # Download it
                        logger.debug(f'download file:{proj}:{subj}:{sess}:{assr}:{res}:{fmatch}')
                        try:
                            _download_file(
                                garjus,
                                proj,
                                subj,
                                sess,
                                assr,
                                res,
                                fmatch,
                                dst
                            )
                        except Exception as err:
                            logger.error(f'{subj}:{sess}:{assr}:{res}:{fmatch}:{err}')
                            raise err
                else:
                    # Download whole resource

                    # Where shall we save it?
                    dst = f'{sess_dir}/{assr}'

                    # Have we already downloaded it?
                    if os.path.exists(os.path.join(dst, res)):
                        logger.debug(f'exists:{dst}')
                        continue

                    # Download it
                    logger.debug(f'download resource:{proj}:{subj}:{sess}:{assr}:{res}')
                    try:
                        _download_resource(
                            garjus,
                            proj,
                            subj,
                            sess,
                            assr,
                            res,
                            dst
                        )
                    except Exception as err:
                        logger.error(f'{subj}:{sess}:{assr}:{res}:{err}')
                        raise err


def download_analysis_inputs(garjus, project, analysis_id, download_dir):
    errors = []

    logger.debug(f'download_analysis_inputs:{project}:{analysis_id}:{download_dir}')

    # Make the output directory
    _make_dirs(download_dir)

    # Determine what we need to download
    analysis = garjus.load_analysis(project, analysis_id)

    logging.info('loading project data')
    assessors = garjus.assessors(projects=[project])
    scans = garjus.scans(projects=[project])

    sessions = pd.concat([
        _sessions_from_scans(scans),
        _sessions_from_assessors(assessors)
    ])
    sessions = sessions.drop_duplicates()

    # Which subjects to include?
    subjects = analysis['analysis_include'].splitlines()

    logger.debug(f'subjects={subjects}')

    # What to download for each subject?
    subj_spec = analysis['processor']['inputs']['xnat']['subjects']

    logger.debug(f'subject spec={subj_spec}')

    for subj in subjects:
        logger.debug(f'subject={subj}')

        # Make the Subject download folder
        subj_dir = f'{download_dir}/{subj}'
        _make_dirs(subj_dir)

        # Download the subject as specified in subj_spec
        try:
            logger.debug(f'_download_subject={subj}')
            _download_subject(
                garjus,
                subj_dir,
                subj_spec,
                project,
                subj,
                sessions,
                assessors)
        except Exception:
            errors.append(subj)
            continue

    # report what's missing
    logger.info(f'errors{errors}')

    logger.debug('done!')
