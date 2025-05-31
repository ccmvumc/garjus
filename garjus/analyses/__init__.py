"""Analyses."""
import logging
import os
import re
import shutil
import tempfile
import subprocess as sb
import yaml
import zipfile
from datetime import datetime
import zipfile
import glob

import pandas as pd
import requests


logger = logging.getLogger('garjus.analyses')


# Files are downloaded into hierarchy,
# for session assessors:
# /INPUTS/<SUBJECT>/<SESSION>/assessors/<ASSESSOR>/<FILES>
# for subject assessors:
# /INPUTS/<SUBJECT>/assessors/<ASSESSOR>/<FILES>


class Analysis(object):
    def __init__(self, project, subjects, repo, csvfile=None, yamlfile=None, imagedir=None):
        self._project = project
        self._subjects = subjects
        self._csvfile = csvfile
        self._repo = repo
        self._imagedir = imagedir

        if yamlfile:
            self._yamlfile = yamlfile
            self._processor = self.load_yaml()
        else:
            self._processor = self.load_processor()

    def load_yaml(self):
        # Load yaml contents
        yaml_file = self._yamlfile
        logger.info(f'loading yamlfile:{yaml_file}')
        try:
            with open(yaml_file, "r") as f:
                return yaml.load(f, Loader=yaml.FullLoader)
        except yaml.error.YAMLError as err:
            logger.error(f'failed to load yaml:{yaml_file}:{err}')

    def load_processor(self):
        filename = 'processor.yaml'

        if self._repo.endswith(filename):
            self._yamlfile = self._repo
            self._repo = os.path.abspath(os.path.join(self._repo, '..', '..', '..'))
            return self.load_yaml()
        elif self._repo.startswith('/'):
            self._yamlfile = f'{self._repo}/{filename}'
            return self.load_yaml()
        else:
            # Load processor yaml from repo
            base = 'https://raw.githubusercontent.com'

            p = self._repo.replace(':', '/').split('/')
            if len(p) != 2:
                logger.error(f'failed to parse:{self._repo}')
                return None

            user = p[0]
            repo = p[1]

            logger.info(f'loading:{user=}:{repo=}')

            url = f'{base}/{user}/{repo}/main/{filename}'

            logger.info(f'{url=}')

            # Get the file contents
            try:
                r = requests.get(url, allow_redirects=True)
                if r.content == '':
                    raise Exception('error exporting file from github')
            except Exception as err:
                logger.error(f'downloading file:{err}')
                return None

            # Cache these for downloading repo later
            self._repo_user = user
            self._repo_name = repo

            return yaml.safe_load(r.text)

    def download_covars(self, garjus, inputs_dir):
        try:
            df = garjus.subjects(self._project)
            #print('renaming')
            df.index.name = 'id'
            #print(df)
            df.to_csv(f'{inputs_dir}/covariates.csv')
        except Exception as err:
            logger.error(f'downloading:{err}')
            return None

    def download_repo(self, repo_dir):
        #curl -sL $REPO | tar zxvf - -C $REPODIR --strip-components=1
        user = self._repo_user
        repo = self._repo_name
        branch = 'main'
        url = f'https://github.com/{user}/{repo}/archive/{branch}.zip'
        repo_zip = f'{repo_dir}/{branch}.zip'

        logger.info(f'loading:{url=}')

        # Get the file contents
        try:
            r = requests.get(url, allow_redirects=True)
            if r.content == '':
                raise Exception('error exporting file from github')
        except Exception as err:
            logger.error(f'downloading file:{err}')
            return None

        # Save it
        if r.status_code == 200:
            with open(repo_zip, 'wb') as f:
                f.write(r.content)
        else:
            logger.error(f'download failed:{r.text}')

        # Unzip it
        with zipfile.ZipFile(repo_zip, 'r') as z:
            z.extractall(repo_dir)

    def run(self, garjus, jobdir):
        jobdir = os.path.abspath(jobdir)

        inputs_dir = f'{jobdir}/INPUTS'
        outputs_dir = f'{jobdir}/OUTPUTS'

        logger.info(f'creating INPUTS and OUTPUTS in:{jobdir}')
        _make_dirs(inputs_dir)
        _make_dirs(outputs_dir)

        # Download inputs
        logger.info(f'downloading analysis inputs to {inputs_dir}')
        self.download_inputs(garjus, inputs_dir)

        # Get the code
        logger.info(f'repo={self._repo}')
        if os.path.exists(self._repo):
            repo_dir = self._repo
            logger.info(f'using local repo:{repo_dir}')
        else:
            # Download repository
            repo_dir = f'{jobdir}/REPO'
            logger.info('downloading repo')
            self.download_repo(repo_dir)

        # Copy covars
        if self._csvfile and os.path.exists(self._csvfile):
            #print('copy csv')
            shutil.copy(self._csvfile, f'{jobdir}/INPUTS/covariates.csv')
        else:
            # Download covars
            self.download_covars(garjus, inputs_dir)

        # Run all commands
        self.run_commands(jobdir, repo_dir)

    def download_inputs(self, garjus, inputs_dir):
        errors = []
        processor = self._processor
        project = self._project
        subjects = self._subjects

        logger.info('loading project data')
        assessors = garjus.assessors(projects=[project])
        scans = garjus.scans(projects=[project])
        sgp = garjus.subject_assessors(projects=[project])

        sessions = pd.concat([
            _sessions_from_scans(scans),
            _sessions_from_assessors(assessors)
        ])
        sessions = sessions.drop_duplicates()

        if not subjects:
            # Default to all subjects
            subjects = list(sessions.SUBJECT.unique())

        logger.info(f'subjects={subjects}')

        # What to download for each subject?
        subj_spec = processor['inputs']['xnat']['subjects']

        logger.debug(f'subject spec={subj_spec}')

        for subj in subjects:
            logger.debug(f'subject={subj}')

            # Make the Subject download folder
            subj_dir = f'{inputs_dir}/{subj}'
            _make_dirs(subj_dir)

            # Download the subject as specified in subj_spec
            try:
                logger.info(f'_download_subject={subj}')
                _download_subject(
                    garjus,
                    subj_dir,
                    subj_spec,
                    project,
                    subj,
                    sessions,
                    assessors,
                    sgp,
                    scans)
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

    def run_commands(self, jobdir, repodir=None):
        command_mode = None
        command = None
        precommand = None
        post = None
        container = None
        extraopts = None
        args = None
        command_type = None
        processor = self._processor

        # Find container command
        if shutil.which('docker'):
            command_mode = 'docker'
        elif shutil.which('singularity'):
            command_mode = 'singularity'
        else:
            logger.error('docker/singularity not found, cannot run containers')
            return

        if command_mode is None:
            logger.debug('no command mode found')
            return

        command = processor.get('command', None)
        if command is None:
            logger.debug('no command found')
            return

        # Run steps
        logger.info('running analysis steps...')

        # Pre command
        precommand = processor.get('pre', None)
        if precommand:
            # Get the container name or path
            container = precommand['container']
            for c in processor['containers']:
                if c['name'] == container:
                    container = c['source']

            extraopts = precommand.get('extraopts', '')
            args = precommand.get('args', '')
            command_type = precommand.get('type', '')

            logger.info(f'running analysis pre-command:{precommand=}')

            if self._imagedir:
                container = f'{self._imagedir}/{container}'

            _run_command(
                container,
                extraopts,
                args,
                command_mode,
                command_type,
                jobdir,
                repodir
            )

        # And now the main command must run
        container = command['container']
        for c in processor['containers']:
            if c['name'] == container:
                if 'source' in c:
                    container = c['source']
                else:
                    raise Exception('cannot run in this environment.')

        logger.debug(f'command mode is {command_mode}')

        extraopts = command.get('extraopts', '')
        args = command.get('args', '')
        command_type = command.get('type', '')

        if self._imagedir:
            container = f'{self._imagedir}/{container}'

        logger.info(f'running main command:{command=}')

        _run_command(
            container,
            extraopts,
            args,
            command_mode,
            command_type,
            jobdir,
            repodir
        )

        # Post command
        post = processor.get('post', None)
        if post:
            # Get the container name or path
            container = post['container']
            for c in processor['containers']:
                if c['name'] == container:
                    container = c['source']

            extraopts = post.get('extraopts', '')
            args = post.get('args', '')
            command_type = post.get('type', '')

            if self._imagedir:
                container = f'{self._imagedir}/{container}'

            logger.info(f'running post command:{post=}')

            _run_command(
                container,
                extraopts,
                args,
                command_mode,
                command_type,
                jobdir,
                repodir
            )


def parse_list(csv_string):
    """
    Split string on commas including any leading/trailing spaces with split
    """
    return re.split(r'\s*,\s*', csv_string)

def _download_zip(xnat, uri, zipfile):
    # Build the uri to download
    _uri = uri + '?format=zip&structure=simplified'

    response = xnat.get(_uri, stream=True)

    if response.status_code != 200:
        raise FileNotFoundError(uri)

    with open(zipfile, 'wb') as f:
        shutil.copyfileobj(response.raw, f)

    return zipfile


def _download_file_stream(xnat, uri, dst):

    response = xnat.get(uri, stream=True)

    logger.debug(f'download response code:{response.status_code}')

    if response.status_code != 200:
        raise FileNotFoundError(uri)

    if dst.endswith('.txt'):
        # Write text as text
        with open(dst, 'w') as f:
            f.write(response.text)
    else:
        # Copy binary file contents
        with open(dst, 'wb') as f:
            shutil.copyfileobj(response.raw, f)

    return dst


def _run_command(
    container,
    extraopts,
    args,
    command_mode,
    command_type,
    jobdir,
    repodir=None
):
    cmd = None

    if not repodir:
        # Mount to top-level of unextracted zipfile
        repodir = glob.glob(f'{jobdir}/REPO/*/')[0]

    # Build the command string
    if command_mode == 'docker':
        if container.startswith('docker://'):
            # Remove docker prefix
            container = container.split('docker://')[1]

        cmd = 'docker'

        if extraopts:
            extraopts = extraopts.replace('-B', '-v')
            logger.info(f'{extraopts=}')

        if command_type == 'singularity_exec':
            cmd += ' run --rm --entrypoint ""'
        else:
            cmd += ' run'

        cmd += f' -v {jobdir}/INPUTS:/INPUTS'
        cmd += f' -v {jobdir}/OUTPUTS:/OUTPUTS'
        cmd += f' -v {repodir}:/REPO'
        cmd += f' {extraopts} {container} {args}'
    elif command_mode == 'singularity':
        if container.startswith('docker://'):
            # Remove docker prefix
            container = container.split('docker://')[1]

        if command_type == 'singularity_exec':
            cmd += 'singularity exec'
        else:
            cmd += 'singularity run'

        cmd += f' -B {jobdir}/INPUTS:/INPUTS'
        cmd += f' -B {jobdir}/OUTPUTS:/OUTPUTS'
        cmd += f' -B {repodir}:/REPO'
        cmd += f' {extraopts} {container} {args}'

    if not cmd:
        logger.debug('invalid command')
        return

    # Run it
    logger.info(cmd)
    os.system(cmd)


def run_analysis(
    garjus,
    project,
    subjects,
    repo,
    jobdir,
    csv=None,
    yamlfile=None,
    imagedir=None,
):
    # Run it
    logger.info(f'running analysis')
    Analysis(project, subjects, repo, csv, yamlfile, imagedir).run(
        garjus, jobdir)

    # That is all
    logger.info(f'analysis done!')


def _sessions_from_scans(scans):
    return scans[[
        'PROJECT',
        'SUBJECT',
        'SESSION',
        'SESSTYPE',
        'MODALITY',
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


def _download_scan_file(garjus, proj, subj, sess, scan, res, fmatch, dst):
    # Make the folders for this file path
    _make_dirs(os.path.dirname(dst))

    # Connect to the resource on xnat
    r = garjus.xnat().select_scan_resource(proj, subj, sess, scan, res)

    # TODO: apply regex or wildcards in fmatch
    # res_obj.files()[0].label()).get(fpath)
    # res.files().label()

    r.file(fmatch).get(dst)

    return dst


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

    logger.debug(f'download zip:{respath}:{reszip}')

    # Download the resource as a zip file
    _download_zip(xnat, respath, reszip)

    # Unzip the file to output dir
    logger.debug(f'unzip file {reszip} to {outdir}')
    with zipfile.ZipFile(reszip) as z:
        z.extractall(outdir)

    # Delete the zip
    os.remove(reszip)


def _download_sgp_file(garjus, proj, subj, assr, res, fmatch, dst):
    # Make the folders for this file path
    _make_dirs(os.path.dirname(dst))

    # Download the file
    uri = f'data/projects/{proj}/subjects/{subj}/experiments/{assr}/resources/{res}/files/{fmatch}'
    _download_file_stream(garjus.xnat(), uri, dst)


def _download_sess_file(garjus, proj, subj, sess, assr, res, fmatch, dst):
    # Make the folders for this file path
    _make_dirs(os.path.dirname(dst))

    # Download the file
    uri = f'data/projects/{proj}/subjects/{subj}/experiments/{sess}/assessors/{assr}/resources/{res}/files/{fmatch}'
    logger.debug(uri)
    _download_file_stream(garjus.xnat(), uri, dst)


def _download_first_file(garjus, proj, subj, sess, scan, res, dst):
    # Make the folders for this file path
    _make_dirs(os.path.dirname(dst))

    # Get name of the first file
    src = garjus.xnat().select_scan_resource(
        proj, subj, sess, scan, res).files().get()[0]

    # Download the file
    uri = f'data/projects/{proj}/subjects/{subj}/experiments/{sess}/scans/{scan}/resources/{res}/files/{src}'
    logger.debug(uri)
    _download_file_stream(garjus.xnat(), uri, dst)


def download_sgp_resources(garjus, project, download_dir, proctype, resources, files):

    assessors = garjus.subject_assessors(
        projects=[project],
        proctypes=[proctype]
    )

    assessors = assessors[assessors.PROCSTATUS == 'COMPLETE']

    for i, a in assessors.iterrows():
        proj = a.PROJECT
        subj = a.SUBJECT
        assr = a.ASSR
        dst = f'{download_dir}/{assr}'

        for res in resources:
            # check if it exists


            if files:
                # Download files
                for fmatch in files:
                    # Have we already downloaded it?
                    if os.path.exists(dst):
                        logger.debug(f'exists:{dst}')
                        continue

                    # Download it
                    logger.info(f'download file:{assr}:{res}:{fmatch}')
                    try:
                        _download_sgp_file(
                            garjus,
                            proj,
                            subj,
                            assr,
                            res,
                            fmatch,
                            f'{dst}/{res}/{fmatch}'
                        )
                    except Exception as err:
                        logger.error(f'{subj}:{assr}:{res}:{fmatch}:{err}')
                        import traceback
                        traceback.print_exc()
                        raise err
            else:
                logger.debug(f'{proj}:{subj}:{assr}:{res}:{dst}')
                _download_sgp_resource_zip(
                    garjus.xnat(),
                    proj,
                    subj,
                    assr,
                    res,
                    dst)


def download_resources(
    garjus,
    project,
    download_dir,
    proctype,
    resources,
    files,
    sesstypes,
    analysis_id=None,
    sessinclude=None
):

    logger.debug(f'loading data:{project}:{proctype}')

    assessors = garjus.assessors(
        projects=[project],
        proctypes=[proctype],
        sesstypes=sesstypes)

    if sessinclude:
        assessors = assessors[assessors.SESSION.isin(sessinclude)]

    if analysis_id:
        # Get list of subjects for specified analysis and apply as filter
        logger.info(f'analysis={analysis_id}')

        # Get the subject list from the analysis
        a = garjus.load_analysis(project, analysis_id)
        _subjects = a['SUBJECTS'].splitlines()
        logger.debug(f'applying subject filter to include:{_subjects}')
        assessors = assessors[assessors.SUBJECT.isin(_subjects)]

    if assessors.empty and not sesstypes:
        logger.info('loading as sgp')
        return download_sgp_resources(
            garjus, project, download_dir, proctype, resources, files)

    assessors = assessors[assessors.PROCSTATUS == 'COMPLETE']

    for i, a in assessors.iterrows():
        proj = a.PROJECT
        subj = a.SUBJECT
        sess = a.SESSION
        assr = a.ASSR
        dst = f'{download_dir}/{assr}'

        for res in resources:
            # check if it exists

            if files:
                # Download files
                for fmatch in files:
                    # Have we already downloaded it?
                    if os.path.exists(dst):
                        logger.debug(f'exists:{dst}')
                        continue

                    # Download it
                    logger.info(f'download file:{assr}:{res}:{fmatch}')
                    try:
                        _download_sess_file(
                            garjus,
                            proj,
                            subj,
                            sess,
                            assr,
                            res,
                            fmatch,
                            f'{dst}/{res}/{fmatch}'
                        )
                    except Exception as err:
                        logger.error(f'{subj}:{assr}:{res}:{fmatch}:{err}')
                        import traceback
                        traceback.print_exc()
                        raise err
            else:
                logger.debug(f'{proj}:{subj}:{sess}:{assr}:{res}:{dst}')
                try:
                    _download_resource(garjus, proj, subj, sess, assr, res, dst)
                except Exception as err:
                    logger.info(f'failed to download:{assr}:{res}')
                    continue


def download_scan_resources(
    garjus, project, download_dir, scantype, resources, files, sesstypes, sessinclude=None):
    logger.debug(f'loading data:{project}:{scantype}')
    scans = garjus.scans(
        projects=[project], scantypes=[scantype], sesstypes=sesstypes)

    if sessinclude:
        scans = scans[scans.SESSION.isin(sessinclude)]

    scans = scans[scans.QUALITY != 'unusable']

    for i, s in scans.iterrows():
        proj = s.PROJECT
        subj = s.SUBJECT
        sess = s.SESSION
        scan = s.SCANID
        dst = f'{download_dir}/{proj}/{subj}/{sess}/{scan}'

        for res in resources:

            # check if it exists
            if res not in s.RESOURCES.split(','):
                logger.debug(f'no resource:{proj}:{subj}:{sess}:{scan}:{res}')
                continue

            if files:
                # Download files
                for fmatch in files:
                    # Have we already downloaded it?
                    if os.path.exists(dst):
                        logger.debug(f'exists:{dst}')
                        continue

                    # Download it
                    logger.info(f'download file:{scan}:{res}:{fmatch}')
                    try:
                        _download_scan_file(
                            garjus,
                            proj,
                            subj,
                            sess,
                            scan,
                            res,
                            fmatch,
                            f'{dst}/{res}/{fmatch}'
                        )
                    except Exception as err:
                        logger.error(f'{subj}:{sess}:{scan}:{res}:{fmatch}:{err}')
                        import traceback
                        traceback.print_exc()
                        raise err
            else:
                logger.debug(f'downloading:{proj}:{subj}:{sess}:{scan}:{res}:{dst}')
                _download_scan_resource(garjus, proj, subj, sess, scan, res, dst)


def _download_resource(garjus, proj, subj, sess, assr, res, dst):
    # Make the folders for destination path
    logger.debug(f'makedirs:{dst}')
    _make_dirs(dst)

    # Connect to the resource on xnat
    logger.debug(f'connecting to resource:{proj}:{subj}:{sess}:{assr}:{res}')
    r = garjus.xnat().select_assessor_resource(proj, subj, sess, assr, res)

    # Download resource and extract
    logger.debug(f'downloading to:{dst}')
    r.get(dst, extract=True)

    return dst


def _download_scan_resource(garjus, proj, subj, sess, scan, res, dst):
    # Make the folders for destination path
    logger.debug(f'makedirs:{dst}')
    _make_dirs(dst)

    # Connect to the resource on xnat
    logger.debug(f'connecting to resource:{proj}:{subj}:{sess}:{scan}:{res}')
    r = garjus.xnat().select_scan_resource(proj, subj, sess, scan, res)

    # Download resource and extract
    logger.debug(f'downloading to:{dst}')
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
                logger.debug(f'skip assr, no match={assr}:{a.PROCTYPE}')
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
                        dst = f'{subj_dir}/assessors/{assr}/{fmatch}'

                        # Have we already downloaded it?
                        if os.path.exists(dst):
                            logger.debug(f'exists:{dst}')
                            continue

                        # Download it
                        logger.info(f'download file:{assr}:{res}:{fmatch}')
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
                    dst = subj_dir

                    # Have we already downloaded it?
                    if os.path.exists(os.path.join(dst, assr, res)):
                        logger.debug(f'exists:{dst}/{assr}/{res}')
                        continue

                    # Download it
                    logger.info(f'download resource:{subj}:{assr}:{res}')
                    try:
                        _download_sgp_resource_zip(
                            garjus.xnat(),
                            proj,
                            subj,
                            assr,
                            res,
                            dst)

                    except Exception as err:
                        logger.error(f'{subj}:{assr}:{res}:{err}')
                        raise err


def _download_subject(
    garjus,
    subj_dir,
    subj_spec,
    proj,
    subj,
    sessions,
    assessors,
    sgp,
    scans
):

    #  subject-level assessors
    sgp_spec = subj_spec.get('assessors', None)
    if sgp_spec:
        logger.info(f'download_sgp={subj_dir}')
        _download_subject_assessors(
            garjus,
            subj_dir,
            sgp_spec,
            proj,
            subj,
            sgp)

    # Download the subjects sessions
    for sess_spec in subj_spec.get('sessions', []):

        if sess_spec.get('select', '') == 'first-mri':
            subj_mris = sessions[(sessions.SUBJECT == subj) & (sessions.MODALITY == 'MR')]
            if len(subj_mris) < 1:
                logger.debug('mri not found')
                return

            sess = subj_mris.SESSION.iloc[0]

            sess_dir = f'{subj_dir}/{sess}'
            logger.info(f'download_session={sess_dir}')
            _download_session(
                garjus,
                sess_dir,
                sess_spec,
                proj,
                subj,
                sess,
                assessors,
                scans)

        elif 'types' in sess_spec:
            sess_types = parse_list(sess_spec['types'])

            for i, s in sessions[sessions.SUBJECT == subj].iterrows():
                sess = s.SESSION

                # Apply session type filter
                if s.SESSTYPE not in sess_types:
                    logger.debug(f'skip session, no match={sess}:{s.SESSTYPE}')
                    continue

                sess_dir = f'{subj_dir}/{sess}'
                logger.info(f'download_session={sess_dir}')
                _download_session(
                    garjus,
                    sess_dir,
                    sess_spec,
                    proj,
                    subj,
                    sess,
                    assessors,
                    scans)
        else:
            for i, s in sessions[sessions.SUBJECT == subj].iterrows():
                sess = s.SESSION

                sess_dir = f'{subj_dir}/{sess}'
                logger.info(f'download_session={sess_dir}')
                _download_session(
                    garjus,
                    sess_dir,
                    sess_spec,
                    proj,
                    subj,
                    sess,
                    assessors,
                    scans)


def _download_scans(
    garjus,
    sess_dir,
    sess_spec,
    proj,
    subj,
    sess,
    scans
):

    # get the scans for this session
    sess_scans = scans[scans.SESSION == sess]

    for k, s in sess_scans.iterrows():
        scan = s.SCANID

        for scan_spec in sess_spec.get('scans', []):
            logger.debug(f'scan_spec={scan_spec}')

            scan_types = scan_spec['types'].split(',')

            logger.debug(f'scan_types={scan_types}')

            if s.SCANTYPE not in scan_types:
                logger.debug(f'skip scan, no match={scan}:{s.SCANTYPE}')
                continue

            # Get list of resources to download from this scan
            resources = scan_spec.get('resources', [])

            # Check for nifti tag
            if 'nifti' in scan_spec:
                # Add a NIFTI resource using value as fdest
                resources.append({
                    'resource': 'NIFTI',
                    'fdest': scan_spec['nifti']
                })

            for res_spec in resources:
                try:
                    res = res_spec['resource']
                except (KeyError, ValueError) as err:
                    logger.error(f'reading resource:{err}')
                    continue

                if 'fdest' in res_spec:
                    fdest = res_spec['fdest']
                    logger.debug(f'setting fdest:{fdest}')
                    dst = f'{os.path.dirname(sess_dir)}/{fdest}'

                    # Have we already downloaded it?
                    if os.path.exists(dst):
                        logger.debug(f'exists:{dst}')
                        continue

                    # Download it
                    logger.info(f'download:{sess}:{scan}:{res}')
                    try:
                        _download_first_file(
                            garjus,
                            proj,
                            subj,
                            sess,
                            scan,
                            res,
                            dst)
                    except Exception as err:
                        logger.error(f'{sess}:{scan}:{res}:first:{err}')
                        raise err
                elif 'fmatch' in res_spec:
                    # Download files
                    for fmatch in res_spec['fmatch'].split(','):

                        # Where shall we save it?
                        dst = f'{sess_dir}/{scan}/{fmatch}'

                        # Have we already downloaded it?
                        if os.path.exists(dst):
                            logger.debug(f'exists:{dst}')
                            continue

                        # Download it
                        logger.info(f'download:{sess}:{scan}:{res}:{fmatch}')
                        try:
                            _download_scan_file(
                                garjus,
                                proj,
                                subj,
                                sess,
                                scan,
                                res,
                                fmatch,
                                dst
                            )
                        except Exception as err:
                            logger.error(f'{sess}:{scan}:{res}:{fmatch}:{err}')
                            raise err
                else:
                    # Download whole resource

                    # Where shall we save it?
                    dst = f'{sess_dir}/{scan}'

                    # Have we already downloaded it?
                    if os.path.exists(os.path.join(dst, res)):
                        logger.debug(f'exists:{dst}')
                        continue

                    # Download it
                    logger.info(f'download resource:{sess}:{scan}:{res}')
                    try:
                        _download_scan_resource(
                            garjus,
                            proj,
                            subj,
                            sess,
                            scan,
                            res,
                            dst
                        )
                    except Exception as err:
                        logger.error(f'{subj}:{sess}:{scan}:{res}:{err}')
                        raise err


def _download_session(
    garjus,
    sess_dir,
    sess_spec,
    proj,
    subj,
    sess,
    assessors,
    scans
):

    if 'scans' in sess_spec:
        _download_scans(garjus, sess_dir, sess_spec, proj, subj, sess, scans)

    # get the assessors for this session
    sess_assessors = assessors[assessors.SESSION == sess]

    for k, a in sess_assessors.iterrows():
        assr = a.ASSR

        for assr_spec in sess_spec.get('assessors', []):
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
                        if 'fdest' in res_spec:
                            print(f'{_fdest=}')
                            _fdest = res_spec['fdest']
                            dst = f'{sess_dir}/assessors/{assr}/{_fdest}'
                        else:
                            dst = f'{sess_dir}/assessors/{assr}/{fmatch}'

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
