"""Analyses."""
import logging
import os
import shutil
import tempfile
import subprocess as sb
import yaml
import zipfile
from datetime import datetime

import pandas as pd


logger = logging.getLogger('garjus.analyses')
x

# TODO: move some of these functions to xnat_utils or to garjus methods

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


def update(garjus, projects=None):
    """Update analyses."""
    for p in (projects or garjus.projects()):
        if p in projects:
            logger.debug(f'updating analyses:{p}')
            _update_project(garjus, p)


def _update_project(garjus, project):
    analyses = garjus.analyses([project], download=True)

    if len(analyses) == 0:
        logger.debug(f'no open analyses for project:{project}')
        return

    # Handle each record
    for i, a in analyses.iterrows():
        aname = a['NAME']

        if not a.get('PROCESSOR', False):
            logger.debug(f'no processor:{aname}')
            continue

        if a['COMPLETE'] != '2':
            logger.debug(f'skipping complete not set:{aname}')
            continue

        if a['STATUS'] == 'READY':
            logger.debug(f'skipping done:{aname}')
            continue

        logger.info(f'updating analysis:{aname}')
        _update(garjus, a)


def _has_outputs(garjus, analysis):
    project = analysis['PROJECT']
    analysis_id = analysis['ID']
    resource = f'{project}_{analysis_id}'
    res_uri = f'/projects/{project}/resources/{resource}'
    output_zip = f'{project}_{analysis_id}_OUTPUTS.zip'

    res = garjus.xnat().select(res_uri)

    file_list = res.files()
    if output_zip in file_list:
        logger.info(f'found:{output_zip}')
        return True
    else:
        return False


def _has_inputs(garjus, analysis):
    project = analysis['PROJECT']
    analysis_id = analysis['ID']
    resource = f'{project}_{analysis_id}'
    res_uri = f'/projects/{project}/resources/{resource}'
    inputs_zip = f'{project}_{analysis_id}_INPUTS.zip'

    res = garjus.xnat().select(res_uri)

    file_list = res.files()
    if inputs_zip in file_list:
        logger.info(f'found:{inputs_zip}')
        return True
    else:
        logger.info(f'inputs not found:{res_uri}:{inputs_zip}')

        return False


def _update(garjus, analysis):
    with tempfile.TemporaryDirectory() as tempdir:
        inputs_dir = f'{tempdir}/INPUTS'
        outputs_dir = f'{tempdir}/OUTPUTS'

        if _has_outputs(garjus, analysis):
            logger.debug(f'outputs exist')
        elif _has_inputs(garjus, analysis):
            logger.debug(f'inputs exist')
        else:
            _make_dirs(inputs_dir)
            _make_dirs(outputs_dir)

            # Create new inputs
            logger.info(f'downloading analysis inputs to {inputs_dir}')
            _download_inputs(garjus, analysis, inputs_dir)

            logger.info(f'uploading analysis inputs zip')
            try:
                dst = upload_inputs(
                    garjus,
                    analysis['PROJECT'],
                    analysis['ID'],
                    tempdir)

                logger.debug(f'set analysis inputs')
                garjus.set_analysis_inputs(
                    analysis['PROJECT'],
                    analysis['ID'],
                    dst)
            except Exception as err:
                logger.error(f'upload failed:{err}')
                return

            logger.info(f'running analysis')
            _run(garjus, analysis, tempdir)

            # Set STATUS
            logger.info(f'set analysis status')
            garjus.set_analysis_status(
                analysis['PROJECT'],
                analysis['ID'],
                'READY')

    # That is all
    logger.info(f'analysis done!')


def _run(garjus, analysis, tempdir):
    # Run commmand and upload output

    processor = analysis['PROCESSOR']

    # Determine what container service we are using
    if shutil.which('singularity'):
        command_mode = 'singularity'
    elif shutil.which('docker'):
        command_mode = 'docker'
    else:
        logger.error('command mode not found, cannot run container command')
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
        # Run steps
        logger.debug('running analysis pre-command')

        # Get the container name or path
        container = precommand['container']
        for c in processor['containers']:
            if c['name'] == container:
                if 'path' in c and command_mode == 'singularity':
                    container = c['path']
                else:
                    container = c['source']

        extraopts = precommand.get('extraopts', '')
        args = precommand.get('args', '')
        command_type = precommand.get('type', '')

        _run_command(
            container,
            extraopts,
            args,
            command_mode,
            command_type,
            tempdir)

    # And now the main command must run
    logger.debug(f'running main command mode')

    # Get the container name or path
    container = command['container']
    for c in processor['containers']:
        if c['name'] == container:
            if 'path' in c and command_mode == 'singularity':
                container = c['path']
            elif 'source' in c:
                container = c['source']
            else:
                raise Exception('processor cannot be run in this environment.')

    logger.debug(f'command mode is {command_mode}')

    extraopts = command.get('extraopts', '')
    args = command.get('args', '')
    command_type = command.get('type', '')

    _run_command(
        container,
        extraopts,
        args,
        command_mode,
        command_type,
        tempdir)

    # Post command
    post = processor.get('post', None)
    if post:
        # Run steps
        logger.debug('running analysis post')

        # Get the container name or path
        container = post['container']
        for c in processor['containers']:
            if c['name'] == container:
                if 'path' in c and command_mode == 'singularity':
                    container = c['path']
                else:
                    container = c['source']

        extraopts = post.get('extraopts', '')
        args = post.get('args', '')
        command_type = post.get('type', '')

        _run_command(
            container,
            extraopts,
            args,
            command_mode,
            command_type,
            tempdir)

    # Upload it
    logger.info(f'uploading output')
    dst = upload_outputs(garjus, analysis['PROJECT'], analysis['ID'], tempdir)
    garjus.set_analysis_outputs(analysis['PROJECT'], analysis['ID'], dst)


def _run_command(container, extraopts, args, command_mode, command_type, tempdir):
    cmd = None

    # Build the command string
    if command_mode == 'singularity':
        cmd = 'singularity'

        if command_type == 'singularity_exec':
            cmd += ' exec'
        else:
            cmd += ' run'

        cmd += f' -e --env USER=$USER --env HOSTNAME=$HOSTNAME'
        cmd += f' --home {tempdir}:$HOME'
        cmd += f' -B $HOME/.ssh:$HOME/.ssh'
        cmd += f' -B {tempdir}/INPUTS:/INPUTS'
        cmd += f' -B {tempdir}/OUTPUTS:/OUTPUTS'
        cmd += f' -B {tempdir}:/tmp'
        cmd += f' -B {tempdir}:/dev/shm'
        cmd += f' {extraopts} {container} {args}'

    elif command_mode == 'docker':
        if container.startswith('docker://'):
            # Remove docker prefix
            container = container.split('docker://')[1]

        cmd = f'docker run --rm'
        cmd += f' -v {tempdir}/INPUTS:/INPUTS'
        cmd += f' -v {tempdir}/OUTPUTS:/OUTPUTS'
        cmd += f' {container}'

    if not cmd:
        logger.debug('invalid command')
        return

    # Run it
    logger.info(cmd)
    os.system(cmd)


def finish_analysis(garjus, project, analysis_id, analysis_dir, processor=None):
    '''Finish an analysis where inputs are already downloaded'''
    analysis = garjus.load_analysis(project, analysis_id)

    if processor:
        # override processor with specified file
        try:
            with open(processor, "r") as f:
                analysis['PROCESSOR'] = yaml.load(f, Loader=yaml.FullLoader)
        except yaml.error.YAMLError as err:
            logger.error(f'failed to load yaml file{processor}:{err}')
            return None

    if not analysis['PROCESSOR']:
        logger.error('no processor specified, cannot run')
        return

    outputs_dir = f'{analysis_dir}/OUTPUTS'

    _make_dirs(outputs_dir)

    # Run it
    logger.info(f'running analysis:{project}:{analysis_id}')
    _run(garjus, analysis, analysis_dir)

    # That is all
    logger.info(f'analysis done!')


def run_analysis(garjus, project, analysis_id, output_zip=None, processor=None, jobdir=None):
    analysis = garjus.load_analysis(project, analysis_id)

    if processor:
        # override processor with specified file
        try:
            with open(processor, "r") as f:
                analysis['PROCESSOR'] = yaml.load(f, Loader=yaml.FullLoader)
        except yaml.error.YAMLError as err:
            logger.error(f'failed to load yaml file{processor}:{err}')
            return None

    if not analysis['PROCESSOR']:
        logger.error('no processor specified, cannot run')
        return

    if jobdir:
        logger.debug(f'jobdir={jobdir}')

    with tempfile.TemporaryDirectory(dir=jobdir) as tempdir:

        inputs_dir = f'{tempdir}/INPUTS'
        outputs_dir = f'{tempdir}/OUTPUTS'

        logger.info(f'creating INPUTS and OUTPUTS in:{tempdir}')
        _make_dirs(inputs_dir)
        _make_dirs(outputs_dir)

        # Download inputs
        logger.info(f'downloading analysis inputs to {inputs_dir}')
        _download_inputs(garjus, analysis, inputs_dir)

        # Run it
        logger.info(f'running analysis:{project}:{analysis_id}')
        _run(garjus, analysis, tempdir)

        if output_zip:
            # Zip output
            logger.info(f'zipping output to {output_zip}')
            sb.run(['zip', '-r', output_zip, 'OUTPUTS'], cwd=tempdir)

    # That is all
    logger.info(f'analysis done!')


def upload_outputs(garjus, project, analysis_id, tempdir):
    # Upload output_zip Project Resource on XNAT named with
    # the project and analysis id as PROJECT_ID, e.g. REMBRANDT_1
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    resource = f'{project}_{analysis_id}'
    res_uri = f'/projects/{project}/resources/{resource}'
    outputs_zip = f'{tempdir}/{project}_{analysis_id}_OUTPUTS_{now}.zip'

    # Zip output
    logger.info(f'zipping output to {outputs_zip}')
    sb.run(['zip', '-r', outputs_zip, 'OUTPUTS'], cwd=tempdir)

    logger.debug(f'connecting to xnat resource:{res_uri}')
    res = garjus.xnat().select(res_uri)

    logger.debug(f'uploading file to xnat resource:{outputs_zip}')
    res.file(os.path.basename(outputs_zip)).put(
        outputs_zip,
        overwrite=True,
        params={"event_reason": "analysis upload"})

    uri = f'{garjus.xnat_host()}/data{res_uri}/files/{os.path.basename(outputs_zip)}'

    return uri


def upload_inputs(garjus, project, analysis_id, tempdir):
    # Upload to Project Resource on XNAT named with
    # the project and analysis id as PROJECT_ID, e.g. REMBRANDT_1
    resource = f'{project}_{analysis_id}'
    res_uri = f'/projects/{project}/resources/{resource}'
    inputs_zip = f'{tempdir}/{project}_{analysis_id}_INPUTS.zip'

    logger.info(f'zipping inputs {tempdir} to {inputs_zip}')
    sb.run(['zip', '-r', inputs_zip, 'INPUTS'], cwd=tempdir)

    assert(os.path.isfile(inputs_zip))

    logger.debug(f'connecting to xnat resource:{res_uri}')
    res = garjus.xnat().select(res_uri)

    logger.debug(f'uploading file to xnat resource:{inputs_zip}')
    res.file(os.path.basename(inputs_zip)).put(
        inputs_zip,
        overwrite=True,
        params={"event_reason": "analysis upload"})

    uri = f'{garjus.xnat_host()}/data{res_uri}/files/{os.path.basename(inputs_zip)}'

    return uri


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


def download_resources(garjus, project, download_dir, proctype, resources, files, sesstypes, analysis_id=None, sessinclude=None):

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
                _download_resource(garjus, proj, subj, sess, assr, res, dst)


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
                        dst = f'{subj_dir}/{assr}/{res}/{fmatch}'

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
        logger.debug(f'download_sgp={subj_dir}')
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
            logger.debug(f'download_session={sess_dir}')
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
            sess_types = sess_spec['types'].split(',')

            for i, s in sessions[sessions.SUBJECT == subj].iterrows():
                sess = s.SESSION

                # Apply session type filter
                if s.SESSTYPE not in sess_types:
                    logger.debug(f'skip session, no match={sess}:{s.SESSTYPE}')
                    continue

                sess_dir = f'{subj_dir}/{sess}'
                logger.debug(f'download_session={sess_dir}')
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
                        dst = f'{sess_dir}/{scan}/{res}/{fmatch}'

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


def download_analysis_inputs(garjus, project, analysis_id, download_dir, processor=None):

    logger.debug(f'download_analysis_inputs:{project}:{analysis_id}:{download_dir}')

    analysis = garjus.load_analysis(project, analysis_id)

    if processor:
        # override processor with specified file
        try:
            with open(processor, "r") as f:
                analysis['PROCESSOR'] = yaml.load(f, Loader=yaml.FullLoader)
        except yaml.error.YAMLError as err:
            logger.error(f'failed to load yaml file{processor}:{err}')
            return None

    if not analysis['PROCESSOR']:
        logger.error('no processor specified, cannot download')
        return

    _download_inputs(garjus, analysis, download_dir)


def download_analysis_outputs(garjus, project, analysis_id, download_dir):

    logger.debug(f'download_analysis_outputs:{project}:{analysis_id}:{download_dir}')

    analysis = garjus.load_analysis(project, analysis_id)

    _download_outputs(garjus, analysis, download_dir)


def _download_inputs(garjus, analysis, download_dir):
    errors = []

    project = analysis['PROJECT']

    logger.info('loading project data')
    assessors = garjus.assessors(projects=[project])
    scans = garjus.scans(projects=[project])
    sgp = garjus.subject_assessors(projects=[project])

    sessions = pd.concat([
        _sessions_from_scans(scans),
        _sessions_from_assessors(assessors)
    ])
    sessions = sessions.drop_duplicates()

    # Which subjects to include?
    subjects = analysis['SUBJECTS'].splitlines()

    if not subjects:
        # Default to all subjects
        subjects = list(sessions.SUBJECT.unique())

    logger.debug(f'subjects={subjects}')

    # What to download for each subject?
    subj_spec = analysis['PROCESSOR']['inputs']['xnat']['subjects']

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


def _download_outputs(garjus, analysis, download_dir):
    project = analysis['PROJECT']
    analysis_id = analysis['ID']
    resource = f'{project}_{analysis_id}'
    res_uri = f'/projects/{project}/resources/{resource}'

    res = garjus.xnat().select(res_uri)

    file_list = res.files().get()
    file_list = [x for x in file_list if 'OUTPUTS' in x]
    file_list = sorted(file_list)

    if len(file_list) < 1:
        raise Exception('no outputs found')

    uri = f'/data/{res_uri}/files/{file_list[0]}'
    dst = f'{download_dir}/{file_list[0]}'
    _make_dirs(download_dir)
    _download_file_stream(garjus.xnat(), uri, dst)
