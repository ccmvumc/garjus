import shutil
import logging

from dax import cluster

# This is a temporary bridge from garjus 2 dax.

# This must run with access to ccmutils redcap to read the queue
# and access to dax diskq directory to write slurm/processor_spec files.
# It does not need XNAT access nor access to any individual project REDCaps,
# the only REDCap it needs is garjus/ccmutils.

# Read these from REDCap for those where status is JOB_QUEUED
# then set status to JOB_RUNNING. (will already be JOB_RUNNING in XNAT)

# All info needed comes from REDCap, does not read any local files, only
# writes. Should not need to access XNAT.


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

    # should we load the data from the yaml file at this point?

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


def task2dax(
    walltime,
    memreq,
    resdir,
    jobdir,
    job_rungroup,
    xnat_host,
    xnat_user,
    job_template,
    yaml_file,
    cmds):

    batch_file = f'{resdir}/DISKQ/BATCH/{assr}.slurm'
    outlog = f'{resdir}/DISKQ/OUTLOG/{assr}.txt'
    processor_spec_path = f'{resdir}/DISKQ/processor/{assr}'

    logging.info(f'writing batch file:{batch_file}')
    batch = cluster.PBS(
        batch_file,
        outlog,
        cmds,
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
    
    # Does all of this need to go in the REDCap queue, or is some
    # of it assumed per dax instance?
    _write_processor_spec(
        processor_spec_path,
        yaml_file,
        singularity_imagedir,
        job_template,
        user_inputs)

    # Set group ownership
    shutil.chown(batch_file, group='h_vuiisadmin')
    shutil.chown(processor_spec_path, group='h_vuiisadmin')


def garjus2dax(garjus):
    garjus.tasks()

    for i, t in tasks.iterrows():
        print(i, t)