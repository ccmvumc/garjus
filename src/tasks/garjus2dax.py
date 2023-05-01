"""garjus queue 2 dax queue."""
import shutil
import logging
import os
import json

from dax import cluster
from .processors import load_from_yaml

# This is a temporary bridge between garjus and dax.

# This must run with access to garjus redcap to read the queue
# and access to dax diskq directory to write slurm/processor_spec files.
# It does not need XNAT access nor access to any individual project REDCaps,
# the only REDCap it needs is garjus/ccmutils.
# It can run under a user that has access to the diskq directory but
# does not need to be the owner nor does it need to be the user that runs dax,
# it must be able set the files to the user group that runs dax.
# All info needed comes from REDCap, does not read any local files, only
# writes. Should not need to access XNAT.
# Read these from REDCap for those where status is JOB_QUEUED
# then set status to JOB_RUNNING. (will already be JOB_RUNNING in XNAT as 
# set when the assessor was created by garjus update tasks)


JOBDIR = '/tmp'
IMAGEDIR = '/data/mcr/centos7/singularity'
RESDIR = '/nobackup/vuiis_daily_singularity/Spider_Upload_Dir'
RUNGROUP = 'h_vuiis'
HOST = 'https://xnat2.vanderbilt.edu/xnat'
USER = 'daxspider'
TEMPLATE = '/data/mcr/centos7/dax_templates/job_template_v3.txt'


def _write_processor_spec(
    filename,
    yaml_file,
    singularity_imagedir,
    job_template,
    user_inputs=None):

    # Write a file with the path to the base processor and any overrides.
    # The file is intended to be written to diskq using the assessor
    # label as the filename. These paths allow dax to read the yaml file
    # during upload to read the description, etc. and put in PDF.

    with open(filename, 'w') as f:
        # write processor yaml filename
        f.write(f'{yaml_file}\n')

        # write customizations
        if user_inputs:
            for k, v in user_inputs.items():
                f.write(f'{k}={v}\n')

        # singularity_imagedir
        f.write(f'singularity_imagedir={singularity_imagedir}\n')

        # job_template
        f.write(f'job_template={job_template}\n')

        # extra blank line
        f.write('\n')


def _task2dax(garjus, assr, walltime, memreq, yaml_file, user_inputs, inputlist, var2val):
    '''Writes a task to a dax slurm script in the local diskq.'''

    # NOTE: this function does the same work as dax task.build_task()
    # at build_cmds() which calls processor.build_cmds(), at the point where we have
    # var2val and the next step is to get the text...
    # more specifically we are splitting build_cmds() where it calls build_text()

    jobdir = JOBDIR
    imagedir = IMAGEDIR
    resdir = RESDIR
    job_rungroup = RUNGROUP
    xnat_host = HOST
    xnat_user = USER
    job_template = TEMPLATE

    batch_file = f'{resdir}/DISKQ/BATCH/{assr}.slurm'
    outlog = f'{resdir}/DISKQ/OUTLOG/{assr}.txt'
    processor_spec_path = f'{resdir}/DISKQ/processor/{assr}'
    assr_dir = f'{jobdir}/{assr}'
    dstdir = f'{resdir}/{assr}'

    # Check for image dir before we give to dax
    if not os.path.isdir(imagedir):
        raise FileNotFoundError(f'singularity images not found:{imagedir}')

    if not os.path.isdir(resdir):
        raise FileNotFoundError(f'upload directory not found:{resdir}')

    if not os.path.isfile(job_template):
        raise FileNotFoundError(f'job template not found:{job_template}')

    for i in inputlist:
        i['fpath'] = i['fpath'].replace('xnat.vanderbilt', 'xnat2.vanderbilt')

    # Load the processor
    processor = load_from_yaml(
        garjus.xnat(),
        yaml_file,
        user_inputs=user_inputs,
        singularity_imagedir=imagedir,
        job_template=job_template)

    # Build the command text
    cmds = processor.build_text(
        var2val,
        inputlist,
        assr_dir,
        dstdir,
        xnat_host,
        xnat_user)

    logging.info(f'writing batch file:{batch_file}')
    batch = cluster.PBS(
        batch_file,
        outlog,
        [cmds],
        walltime,
        mem_mb=memreq,
        ppn=1,
        env=None,
        email=None,
        email_options='FAIL',
        rungroup=job_rungroup,
        xnat_host=xnat_host,
        job_template=job_template)
    
    batch.write()

    # Write processor spec file for version 3
    logging.info(f'writing processor spec file:{processor_spec_path}')

    _write_processor_spec(
        processor_spec_path,
        yaml_file,
        imagedir,
        job_template,
        user_inputs)

    # Set group ownership
    shutil.chown(batch_file, group='h_vuiisadmin')
    shutil.chown(processor_spec_path, group='h_vuiisadmin')


def queue2dax(garjus):

    tasks = garjus.tasks()

    for i, t in tasks.iterrows():
        assr = t['ASSESSOR']
        status = t['STATUS']

        if status != 'JOB_QUEUED':
            logging.debug(f'skipping:{i}:{assr}:{status}')
            continue

        logging.info(f'{i}:{assr}:{status}')

        walltime = t['WALLTIME']
        memreq = t['MEMREQ']
        inputlist = json.loads(t['INPUTLIST'], strict=False)
        var2val = json.loads(t['VAR2VAL'], strict=False)
        yaml_file = t['YAMLFILE']
        user_inputs = t['USERINPUTS']

        try:       
            _task2dax(
                garjus,
                assr,
                walltime,
                memreq,
                yaml_file,
                user_inputs,
                inputlist,
                var2val)

            garjus.set_task_status(t['PROJECT'], t['ID'], 'JOB_RUNNING')

        except Exception as err:
            logging.error(err)

