""" Spin - take a yaml for a spin, test locally using docker and keep outputs local"""
import logging
import os
import shutil
import yaml

import pandas as pd

from ..analyses import parse_list, _make_dirs, _download_file_stream, _sessions_from_scans, _sessions_from_assessors, \
    _download_scan_resource, _download_scan_file, _download_file, _download_sess_file, _download_resource, _download_scan_resource


logger = logging.getLogger('garjus')


class Spin(object):
    def __init__(self, project, subject, yamlfile, imagedir=None):
        self._project = project
        self._subject = subject
        self._imagedir = imagedir # optional, used with singularity, not docker
        self._yamlfile = yamlfile
        self._processor = self.load_yaml()

    def load_yaml(self):
        # Load yaml contents
        yaml_file = self._yamlfile
        logger.info(f'loading yamlfile:{yaml_file}')
        try:
            with open(yaml_file, "r") as f:
                return yaml.load(f, Loader=yaml.FullLoader)
        except yaml.error.YAMLError as err:
            logger.error(f'failed to load yaml:{yaml_file}:{err}')

    def run(self, garjus, jobdir, reuse_inputs=False):
        jobdir = os.path.abspath(jobdir)
        inputs_dir = f'{jobdir}/INPUTS'
        outputs_dir = f'{jobdir}/OUTPUTS'
        var2val = {'subject': self._subject}

        if reuse_inputs:
            logger.info(f'using existing inputs in:{jobdir}/INPUTS')
        else:
            logger.info(f'creating INPUTS in:{jobdir}')
            _make_dirs(inputs_dir)

            # Download inputs
            logger.info(f'downloading inputs to {inputs_dir}')
            self.download_inputs(garjus, inputs_dir)

        logger.info(f'creating OUTPUTS in:{jobdir}')
        _make_dirs(outputs_dir)

        # Run all commands
        self.run_commands(jobdir, var2val)

    def download_inputs(self, garjus, inputs_dir):
        processor = self._processor
        project = self._project
        subject = self._subject

        logger.info('loading project data')
        assessors = garjus.assessors(projects=[project])
        scans = garjus.scans(projects=[project])
        sessions = pd.concat([
            _sessions_from_scans(scans),
            _sessions_from_assessors(assessors)
        ])
        sessions = sessions.drop_duplicates()

        spec = processor['inputs']['xnat']

        filters = spec.get('filters', [])

        if spec.get('sessions', False):
            # Handle subject level
            logger.debug(f'subject spec={spec}')

            # Download the subject as specified
            _download_subject(
                garjus,
                inputs_dir,
                spec,
                project,
                subject,
                sessions,
                assessors,
                scans,
                filters)
        else:
            # Handle session level
            try:
                (subj, sess) = sessions[sessions.SESSION == subject].iloc[0][['SUBJECT', 'SESSION']]
            except Exception as err:
                logger.error(f'session not found:{subject}')
                return

            _download_session(
                garjus,
                inputs_dir,
                spec,
                project,
                subj,
                sess,
                assessors,
                scans,
                filters)

        logger.debug('done!')

    def run_commands(self, jobdir, var2val):
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
        logger.info('running steps...')

        # Pre command
        precommand = processor.get('pre', None)
        if precommand:
            # Get the container name or path
            container = precommand['container']
            for c in processor['containers']:
                # Find a match on name
                if c['name'] == container:
                    # Found a match now set based on command mode
                    if command_mode == 'docker':
                        container = c['source']
                    else:
                        container = c['path']

            extraopts = precommand.get('extraopts', '')
            args = precommand.get('args', '')
            command_type = precommand.get('type', '')

            logger.info(f'running pre-command:{precommand=}')

            if self._imagedir:
                container = f'{self._imagedir}/{container}'

            args = args.format(**var2val)

            _run_command(
                container,
                extraopts,
                args,
                command_mode,
                command_type,
                jobdir,
            )

        # And now the main command must run
        container = command['container']
        for c in processor['containers']:
            # Find a match on name
            if c['name'] == container:
                # Found a match now set based on command mode
                try:
                    if command_mode == 'docker':
                        container = c['source']
                    else:
                        container = c['path']
                except:
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
        )

        # Post command
        post = processor.get('post', None)
        if post:
            # Get the container name or path
            container = post['container']
            for c in processor['containers']:
                # Find a match
                if c['name'] == container:
                    # Set base on command mode
                    if command_mode == 'docker':
                        container = c['source']
                    else:
                        container = c['path']

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
            )


def _run_command(
    container,
    extraopts,
    args,
    command_mode,
    command_type,
    jobdir,
):
    cmd = None

    # Build the command string
    if command_mode == 'docker':
        cmd = 'docker'

        if container.startswith('docker://'):
            # Remove docker prefix
            container = container.split('docker://')[1]

        if extraopts:
            extraopts = extraopts.replace('-B', '-v')
            logger.info(f'{extraopts=}')

        if command_type == 'singularity_exec':
            cmd += ' run --rm --entrypoint ""'
        else:
            cmd += ' run'

        cmd += f' -v {jobdir}/INPUTS:/INPUTS'
        cmd += f' -v {jobdir}/OUTPUTS:/OUTPUTS'
        cmd += f' {extraopts} {container} {args}'
    elif command_mode == 'singularity':
        if command_type == 'singularity_exec':
            cmd = 'singularity exec'
        else:
            cmd = 'singularity run'

        cmd += f' -B {jobdir}/INPUTS:/INPUTS'
        cmd += f' -B {jobdir}/OUTPUTS:/OUTPUTS'
        cmd += f' {extraopts} {container} {args}'

    if not cmd:
        logger.debug('invalid command')
        return

    # Run it
    logger.info(cmd)
    os.system(cmd)


def run_spin(
    garjus,
    project,
    subjects,
    jobdir,
    yamlfile=None,
    imagedir=None,
    reuse_inputs=False
):
    # Run it
    logger.info(f'running spin')
    Spin(project, subjects, yamlfile, imagedir).run(garjus, jobdir, reuse_inputs)

    # That is all
    logger.info(f'spin done!')


def _download_scan_first_file(garjus, proj, subj, sess, scan, res, dst):
    # Make the folders for this file path
    _make_dirs(os.path.dirname(dst))

    # Get name of the first file
    src = garjus.xnat().select_scan_resource(
        proj, subj, sess, scan, res).files().get()[0]

    # Download the file
    uri = f'data/projects/{proj}/subjects/{subj}/experiments/{sess}/scans/{scan}/resources/{res}/files/{src}'
    logger.debug(uri)
    _download_file_stream(garjus.xnat(), uri, dst)


def _download_assessor_first_file(garjus, proj, subj, sess, assr, res, dst):
    # Make the folders for this file path
    _make_dirs(os.path.dirname(dst))

    # Get name of the first file
    src = garjus.xnat().select_assessor_resource(
        proj, subj, sess, assr, res).files().get()[0]

    # Download the file
    uri = f'data/projects/{proj}/subjects/{subj}/experiments/{sess}/assessors/{assr}/resources/{res}/files/{src}'
    logger.debug(uri)
    _download_file_stream(garjus.xnat(), uri, dst)


def _download_subject(
    garjus,
    subj_dir,
    subj_spec,
    proj,
    subj,
    sessions,
    assessors,
    scans,
    filters
):
    # Download the subjects sessions
    for i, sess_spec in enumerate(subj_spec.get('sessions', [])):
        if sess_spec.get('select', '') == 'first-mri':
            # Find the firest mri
            subj_mris = sessions[(sessions.SUBJECT == subj) & (sessions.MODALITY == 'MR')]
            if len(subj_mris) < 1:
                logger.debug('mri not found')
                return

            sess = subj_mris.SESSION.iloc[0]

            logger.info(f'download_session={sess}:{subj_dir}')
            _download_session(
                garjus,
                subj_dir,
                sess_spec,
                proj,
                subj,
                sess,
                assessors,
                scans,
                filters)

        elif 'type' in sess_spec:
            sess_types = parse_list(sess_spec['type'])

            # Compare each session by type in spec
            for j, s in sessions[sessions.SUBJECT == subj].iterrows():
                sess = s.SESSION

                # Apply session type filter
                if s.SESSTYPE not in sess_types:
                    logger.debug(f'skip session, no match={sess}:{s.SESSTYPE}')
                    continue

                logger.info(f'download_session={sess}:{subj_dir}')
                _download_session(
                    garjus,
                    subj_dir,
                    sess_spec,
                    proj,
                    subj,
                    sess,
                    assessors,
                    scans,
                    filters)
        else:
            # Download all sessions
            for j, s in sessions[sessions.SUBJECT == subj].iterrows():
                sess = s.SESSION

                logger.info(f'{i}:{j}:download_session={sess}:{subj_dir}')
                _download_session(
                    garjus,
                    subj_dir,
                    sess_spec,
                    proj,
                    subj,
                    sess,
                    assessors,
                    scans,
                    filters)


def _download_scans(
    garjus,
    sess_dir,
    sess_spec,
    proj,
    subj,
    sess,
    scans
):
    spec2val = {}

    # get the scans for this session
    sess_scans = scans[scans.SESSION == sess]

    # Intialize map of scan spec 2 value
    for scan_spec in sess_spec.get('scans', []):
        spec2val[scan_spec['name']] = []

    for k, s in sess_scans.iterrows():
        scan = s.SCANID

        for scan_spec in sess_spec.get('scans', []):
            logger.debug(f'scan_spec={scan_spec}')
            spec_name = scan_spec['name']

            scan_types = scan_spec['types'].split(',')

            logger.debug(f'scan_types={scan_types}')

            if s.SCANTYPE not in scan_types:
                logger.debug(f'skip scan, no match={scan}:{s.SCANTYPE}')
                continue

            spec2val[spec_name].append(s.full_path)

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

                fdest = res_spec.get('fdest', None)
                fmatch = res_spec.get('fmatch', None)

                if fmatch and fdest:
                     # Download files
                    for m, d in zip(fmatch.split(','), fdest.split(',')):

                        # Where shall we save it?
                        dst = f'{sess_dir}/{d}'

                        # Have we already downloaded it?
                        if os.path.exists(dst):
                            logger.debug(f'exists:{dst}')
                            continue

                        # Download it
                        logger.info(f'download:{sess}:{scan}:{res}:{m}:{dst}')
                        try:
                            _download_scan_file(
                                garjus,
                                proj,
                                subj,
                                sess,
                                scan,
                                res,
                                m,
                                dst
                            )
                        except Exception as err:
                            logger.error(f'{sess}:{scan}:{res}:{m}:{err}')
                            raise err

                elif fdest and not fmatch:
                    logger.debug(f'getting fdest:{fdest}')
                    dst = f'{sess_dir}/{fdest}'

                    # Have we already downloaded it?
                    if os.path.exists(dst):
                        logger.debug(f'exists:{dst}')
                        continue

                    # Download it
                    logger.info(f'download:{sess}:{scan}:{res}:{dst}')
                    try:
                        _download_scan_first_file(
                            garjus,
                            proj,
                            subj,
                            sess,
                            scan,
                            res,
                            dst)
                    except Exception as err:
                        logger.error(f'{sess}:{scan}:{res}:{err}')
                        raise err
                elif fmatch and not fdest:
                    # Download files
                    for f in fmatch.split(','):

                        # Where shall we save it?
                        dst = f'{sess_dir}/{f}'

                        # Have we already downloaded it?
                        if os.path.exists(dst):
                            logger.debug(f'exists:{dst}')
                            continue

                        # Download it
                        logger.info(f'download:{sess}:{scan}:{res}:{f}:{dst}')
                        try:
                            _download_scan_file(
                                garjus,
                                proj,
                                subj,
                                sess,
                                scan,
                                res,
                                f,
                                dst
                            )
                        except Exception as err:
                            logger.error(f'{sess}:{scan}:{res}:{f}:{dst}:{err}')
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

    return spec2val


def _match(filters, assr_inputs, assr_name, scan2val):
    # Check for assr name in any of the match filters and confirm the scan input matches scan2val
    for f in filters:
        filter_type = f['type']

        if filter_type != 'match':
            LOGGER.error('invalid filter type:{}'.format(filter_type))
            continue

        # Split the comma-separated list of inputs
        filter_inputs = f['inputs'].split(',')

        # Check each input for matching assr name
        for i in filter_inputs:

            # Find inputs that include this assessor name
            if '/' in i and i.split('/')[0] == assr_name:
                scan_name = i.split('/')[1]
                assr_scan = assr_inputs[scan_name]

                for j in filter_inputs:
                    if j == i:
                        continue

                    if '/' in j:
                        continue

                    if assr_scan not in scan2val[j]:
                        logger.debug(f'NOPE:{i=}:{j=}:{scan_name=}:{assr_scan=}:{scan2val[j]=}')
                        return False

    return True


def _download_session(
    garjus,
    sess_dir,
    sess_spec,
    proj,
    subj,
    sess,
    assessors,
    scans, 
    filters
):

    if 'scans' in sess_spec:
        scan2val = _download_scans(garjus, sess_dir, sess_spec, proj, subj, sess, scans)

    # get the assessors for this session
    sess_assessors = assessors[assessors.SESSION == sess]

    # Filter to only complete assessors
    sess_assessors = sess_assessors[sess_assessors.PROCSTATUS == 'COMPLETE']

    for k, a in sess_assessors.iterrows():
        assr = a.ASSR

        for assr_spec in sess_spec.get('assessors', []):
            logger.debug(f'assr_spec={assr_spec}')

            assr_types = assr_spec['types'].split(',')

            logger.debug(f'assr_types={assr_types}')

            if a.PROCTYPE not in assr_types:
                logger.debug(f'skip assr, no match on type={assr}:{a.PROCTYPE}')
                continue

            # Check matching filters
            if not filters:
                logger.debug('no matching filters')
            elif not _match(filters, a.INPUTS, assr_spec['name'], scan2val):
                logger.debug('no match')
                continue

            for res_spec in assr_spec['resources']:

                try:
                    res = res_spec['resource']
                except (KeyError, ValueError) as err:
                    logger.error(f'reading resource:{err}')
                    continue

                fdest = res_spec.get('fdest', None)
                fmatch = res_spec.get('fmatch', None)

                if fdest and fmatch:
                    print('fdest and fmatch not yet supported')
                elif fdest and not fmatch:
                    fdest = res_spec['fdest']
                    logger.debug(f'setting fdest:{fdest}')
                    dst = f'{sess_dir}/{fdest}'

                    # Have we already downloaded it?
                    if os.path.exists(dst):
                        logger.debug(f'exists:{dst}')
                        continue

                    # Download it
                    logger.info(f'download:{sess}:{assr}:{res}')
                    try:
                        _download_assessor_first_file(
                            garjus,
                            proj,
                            subj,
                            sess,
                            assr,
                            res,
                            dst)
                    except Exception as err:
                        logger.error(f'{sess}:{assr}:{res}:{err}')
                        raise err
                elif fmatch and not fdest:
                    # Download files
                    for f in res_spec['fmatch'].split(','):

                        dst = f'{sess_dir}/{f}'

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
                                f,
                                dst
                            )
                        except Exception as err:
                            logger.error(f'{subj}:{sess}:{assr}:{res}:{f}:{err}')
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
