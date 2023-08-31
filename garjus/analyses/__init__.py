"""Analyses."""
import logging
import os, shutil
import tempfile
import subprocess as sb

import pandas as pd


logger = logging.getLogger('garjus.analyses')



def _download_zip(xnat, uri, zipfile):
    # Build the uri to download
    _uri = uri + '?format=zip&structure=simplified'

    response = xnat.get(_uri, stream=True)
    with open(zipfile, 'wb') as f:
        shutil.copyfileobj(response.raw, f)

    return zipfile


def _download_file_stream(xnat, uri, dst):

    response = xnat.get(uri, stream=True)

    with open(dst, 'wb') as f:
        shutil.copyfileobj(response.raw, f)

    return dst


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

        # First is output complete, already run, do nothing
        # continue

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

    # should we run it?

    # Run it

    # Upload
    #upload_analysis(garjus, project, analysis_id, upload_dir)

    # Set Analysis color to COMPLETE/Green


def run_analysis(garjus, project, analysis_id, output_zip):
    with tempfile.TemporaryDirectory() as tempdir:
        download_dir = f'{tempdir}/INPUTS'
        upload_dir = f'{tempdir}/OUTPUTS'
   
        _make_dirs(upload_dir)

        # Download inputs
        logger.info(f'downloading inputs to {download_dir}')
        download_analysis_inputs(garjus, project, analysis_id, download_dir)

        # Run steps
        logger.info('running analysis steps...')
        # run singularity??? run docker??? make a job? where?
        # ?????????????????????????????????????????????????????????
        cmd = SINGULARITY_CMD



        # Zip output
        logger.info(f'zipping output {upload_dir} to {output_zip}')
        sb.run(['zip', '-r', output_zip, 'OUTPUTS'], cwd=tempdir)
        logger.info(f'analysis done!')


def upload_analysis(garjus, project, analysis_id, output_zip):
    # Upload to xnat

    # Set path to xnat zip in REDCap field

    return


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
    _make_dirs(os.path.dirname(dst))

    # Connect to the resource on xnat
    r = garjus.xnat().select_assessor_resource(proj, subj, sess, assr, res)

    # TODO: apply regex or wildcards in fmatch
    # res_obj.files()[0].label()).get(fpath)
    # res.files().label()

    r.file(fmatch).get(dst)

    return dst


def _download_sgp_resource_zip(xnat, project, subject, assessor, resource, outdir):
    reszip = '{}_{}.zip'.format(assessor, resource)
    respath = 'data/projects/{}/subjects/{}/experiments/{}/resources/{}/files'
    respath = respath.format(project, subject, assessor, resource)

    # Download the resource as a zip file
    download_zip(respath, reszip)

    # Unzip the file to output dir
    logger.debug(f'unzip file {rezip} to {outdir}')
    with zipfile.ZipFile(reszip) as z:
        z.extractall(outdir)

    # TODO: check downloaded files, compare/size/md5 to xnat

    # Delete the zip
    os.remove(reszip)


def _download_sgp_file(garjus, proj, subj, assr, res, fmatch, dst):
    # Make the folders for this file path
    _make_dirs(os.path.dirname(dst))

    # Download the file
    uri = f'data/projects/{proj}/subjects/{subj}/experiments/{assr}/resources/{res}/files/{fmatch}'
    _download_file_stream(garjus.xnat(), uri, dst)


def _download_resource(garjus, proj, subj, sess, assr, res, dst):
    # Make the folders for destination path
    _make_dirs(dst)

    # Connect to the resource on xnat
    r = garjus.xnat().select_assessor_resource(proj, subj, sess, assr, res)

    # Download resource and extract
    r.get(dst, extract=True)

    return dst


def _download_sgp_resource(garjus, proj, subj, assr, res, dst):
    # Make the folders for destination path
    _make_dirs(dst)

    # Connect to the resource on xnat
    r = garjus.xnat().select_sgp_assessor(proj, subj, assr).resource(res)

    # Download extracted
    r.get(dst, extract=True)

    return dst


def _make_dirs(dirname):
    try:
        os.makedirs(dirname)
    except FileExistsError:
        pass


def _download_subject_assessors(garjus, subj_dir, sgp_spec, proj, subj, sgp):

    sgp = sgp[sgp.SUBJECT == subj]

    for k, a in sgp.iterrows():

        assr = a.ASSR

        for assr_spec in sgp_spec:
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
                        dst = f'{subj_dir}/{assr}/{res}/{fmatch}'

                        # Have we already downloaded it?
                        if os.path.exists(dst):
                            logger.debug(f'exists:{dst}')
                            continue

                        # Download it
                        logger.info(f'download file:{proj}:{subj}:{assr}:{res}:{fmatch}')
                        try:
                            _download_sgp_file(
                                garjus,
                                proj,
                                subj,
                                assr,
                                res,
                                fmatch,
                                dst
                            )
                        except Exception as err:
                            logger.error(f'{subj}:{assr}:{res}:{fmatch}:{err}')
                            import traceback
                            traceback.print_exc()
                            raise err
                else:
                    # Download whole resource

                    # Where shall we save it?
                    dst = f'{subj_dir}/{assr}'

                    # Have we already downloaded it?
                    if os.path.exists(os.path.join(dst, res)):
                        logger.debug(f'exists:{dst}')
                        continue

                    # Download it
                    logger.info(f'download resource:{proj}:{subj}:{assr}:{res}')
                    try:
                        _download_sgp_resource(
                            garjus,
                            proj,
                            subj,
                            assr,
                            res,
                            dst
                        )
                    except Exception as err:
                        logger.error(f'{subj}:{assr}:{res}:{err}')
                        raise err


def _download_subject(garjus, subj_dir, subj_spec, proj, subj, sessions, assessors, sgp):

    #  subject-level assessors
    sgp_spec = subj_spec['assessors']
    logger.debug(f'download_sgp={subj_dir}')
    _download_subject_assessors(garjus, subj_dir, sgp_spec, proj, subj, sgp)

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
                        logger.info(f'download file:{proj}:{subj}:{sess}:{assr}:{res}:{fmatch}')
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
                    logger.info(f'download resource:{proj}:{subj}:{sess}:{assr}:{res}')
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
    sgp = garjus.subject_assessors(projects=[project])

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
                assessors,
                sgp)
        except Exception as err:
            logger.debug(err)
            errors.append(subj)
            continue

    # report what's missing
    if errors:
        logger.info(f'errors{errors}')
    else:
        logger.info(f'download complete with no errors!')

    logger.debug('done!')
