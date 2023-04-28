"""dax queue 2 garjus queue."""
import subprocess
from io import StringIO
import logging
import os
from datetime import datetime

import pandas as pd


# This is a temporary bridge between garjus and dax.

USER = 'vuiis_daily_singularity'
SQUEUE = 'squeue -u vuiis_daily_singularity --format="%j|%A|%L|%m|%M|%p|%t|%u|%S|%T|%V|%l|"'
RESDIR = '/nobackup/vuiis_daily_singularity/Spider_Upload_Dir'

JOB_TAB_COLS = [
    'LABEL', 'PROJECT', 'STATUS', 'PROCTYPE', 'USER',
    'JOBID', 'TIME', 'WALLTIME', 'LASTMOD']

SQUEUE_COLS = [
    'NAME', 'ST', 'STATE', 'PRIORITY', 'JOBID', 'MIN_MEMORY',
    'TIME', 'SUBMIT_TIME', 'START_TIME', 'TIME_LIMIT', 'TIME_LEFT', 'USER']

# we concat diskq status and squeue status to make a single status
# squeue states: CG,F, PR, S, ST
# diskq statuses: JOB_RUNNING, JOB_FAILED, NEED_TO_RUN, COMPLETE,
# UPLOADING, READY_TO_COMPLETE, READY_TO_UPLOAD
STATUS_MAP = {
    'COMPLETENONE': 'COMPLETE',
    'JOB_FAILEDNONE': 'FAILED',
    'JOB_RUNNINGCD': 'RUNNING',
    'JOB_RUNNINGCG': 'RUNNING',
    'JOB_RUNNINGF': 'RUNNING',
    'JOB_RUNNINGR': 'RUNNING',
    'JOB_RUNNINGNONE': 'RUNNING',
    'JOB_RUNNINGPD': 'PENDING',
    'NONENONE': 'WAITING',
    'READY_TO_COMPLETENONE': 'COMPLETE',
    'READY_TO_UPLOADNONE': 'COMPLETE'}


def _load_dax_queue():
    logging.debug('loading diskq')
    diskq_df = _load_diskq_queue()

    logging.debug('loading squeue')
    squeue_df = _load_slurm_queue()

    # merge squeue data into task queue
    logging.debug('merging data')

    if diskq_df.empty and squeue_df.empty:
        logging.debug('both empty')
        df = pd.DataFrame(columns=diskq_df.columns.union(squeue_df.columns))
    elif diskq_df.empty:
        logging.debug('diskq empty')
        df = squeue_df.reindex(squeue_df.columns.union(diskq_df.columns), axis=1)
    elif squeue_df.empty:
        logging.debug('squeue empty')
        df = diskq_df.reindex(diskq_df.columns.union(squeue_df.columns), axis=1)
    else:
        df = pd.merge(diskq_df, squeue_df, how='outer', on=['LABEL', 'USER'])

    if not df.empty:
        # assessor label is delimited by "-x-", first element is project,
        # fourth element is processing type
        df['PROJECT'] = df['LABEL'].str.split('-x-', n=1, expand=True)[0]
        df['PROCTYPE'] = df['LABEL'].str.split('-x-', n=4, expand=True)[3]

        # Add some text to avoid blanks in the table
        df['JOBID'].fillna('not in queue', inplace=True)

        # create a concatenated status that maps to full status
        df['psST'] = df['procstatus'].fillna('NONE') + df['ST'].fillna('NONE')
        df['STATUS'] = df['psST'].map(STATUS_MAP).fillna('UNKNOWN')

    # Determine how long ago status changed
    # how long has it been running, pending, waiting or complete?

    # Minimize columns
    logging.debug('finishing data')
    df = df.reindex(columns=JOB_TAB_COLS)

    return df.sort_values('LABEL')

    return df


# Loads the dax queue from disk
def _load_diskq_queue(status=None):
    task_list = list()
    diskq_dir = os.path.join(RESDIR, 'DISKQ')
    batch_dir = os.path.join(diskq_dir, 'BATCH')

    for t in os.listdir(batch_dir):
        assr = os.path.splitext(t)[0]
        logging.debug(f'load task:{assr}')
        task = _load_diskq_task(diskq_dir, assr)
        task['USER'] = USER
        task_list.append(task)

    if len(task_list) > 0:
        df = pd.DataFrame(task_list)
    else:
        df = pd.DataFrame(columns=[
            'LABEL', 'procstatus', 'jobid', 'jobnode', 'jobstartdate',
            'memused', 'walltimeused', 'WALLTIME', 'LASTMOD', 'USER'])

    return df


# Load a single task/job information from disk
def _load_diskq_task(diskq, assr):
    return {
        'LABEL': assr,
        'procstatus': _get_diskq_attr(diskq, assr, 'procstatus'),
        'jobid': _get_diskq_attr(diskq, assr, 'jobid'),
        'jobnode': _get_diskq_attr(diskq, assr, 'jobnode'),
        'jobstartdate': _get_diskq_attr(diskq, assr, 'jobstartdate'),
        'memused': _get_diskq_attr(diskq, assr, 'memused'),
        'walltimeused': _get_diskq_attr(diskq, assr, 'walltimeused'),
        'WALLTIME': _get_diskq_walltime(diskq, assr),
        'LASTMOD': _get_diskq_lastmod(diskq, assr)}


# Load slurm data
def _load_slurm_queue():
    try:
        cmd = SQUEUE
        result = subprocess.run([cmd], shell=True, stdout=subprocess.PIPE)
        _data = result.stdout.decode('utf-8')
        df = pd.read_csv(
            StringIO(_data), delimiter='|', usecols=SQUEUE_COLS)
        df['LABEL'] = df['NAME'].str.split('.slurm').str[0]
        return df
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=SQUEUE_COLS+['LABEL'])


def _get_diskq_walltime(diskq, assr):
    COOKIE = "#SBATCH --time="
    walltime = None
    bpath = os.path.join(diskq, 'BATCH', assr + '.slurm')

    try:
        with open(bpath, 'r') as f:
            for line in f:
                if line.startswith(COOKIE):
                    walltime = line.split('=')[1].replace('"', '').replace("'", '')
                    break
    except IOError:
        logging.warn('file does not exist:' + bpath)
        return None
    except PermissionError:
        logging.warn('permission error reading file:' + bpath)
        return None

    return walltime


def _get_diskq_lastmod(diskq, assr):

    if os.path.exists(os.path.join(diskq, 'procstatus', assr)):
        apath = os.path.join(diskq, 'procstatus', assr)
    elif os.path.exists(os.path.join(diskq, 'BATCH', assr + '.slurm')):
        apath = os.path.join(diskq, 'BATCH', assr + '.slurm')
    else:
        return None

    updatetime = datetime.fromtimestamp(os.path.getmtime(apath))
    delta = datetime.now() - updatetime
    return delta


def _get_diskq_attr(diskq, assr, attr):
    apath = os.path.join(diskq, attr, assr)

    if not os.path.exists(apath):
        return None

    try:
        with open(apath, 'r') as f:
            return f.read().strip()
    except PermissionError:
        return None


def _get_changes(garjus_queue, dax_queue):
    # Make list of (ID,PROJECT,STATUS) where status doesn't match
    df = pd.merge(garjus_queue, dax_queue, left_on='ASSESSOR', right_on='LABEL')
    df = df[df.STATUS_x != df.STATUS_y]
    df = df[['ID', 'PROJECT_x', 'STATUS_y']]
    df = df.rename(columns={'PROJECT_x': 'PROJECT', 'STATUS_y': 'STATUS'})

    return df


def _get_xnat_changes(garjus_queue, assessors):
    # Make list of (ID,PROJECT,STATUS) where status doesn't match
    df = pd.merge(garjus_queue, assessors, left_on='ASSESSOR', right_on='ASSR')

    # Only where status mismatch
    df = df[df.STATUS != df.PROCSTATUS]

    # Subset of columns we need
    df = df[['ID', 'PROJECT_x', 'PROCSTATUS']]

    # Don't revert to JOB_RUNNING
    df = df[df.PROCSTATUS != 'JOB_RUNNING']

    # Use column names expected by garjus
    df = df.rename(columns={'PROJECT_x': 'PROJECT', 'PROCSTATUS': 'STATUS'})

    return df


def dax2queue(garjus):
    # load diskq, run squeue to get updates, compare to queue, apply changes

    resdir = RESDIR

    if not os.path.isdir(resdir):
        raise FileNotFoundError(f'upload directory not found:{resdir}')

    gqueue = garjus.tasks()

    dqueue = _load_dax_queue()

    # Filter projects
    dqueue = dqueue[dqueue.PROJECT.isin(garjus.projects())]

    # Get just the changes to apply
    df = _get_changes(gqueue, dqueue)

    # Apply changes
    if df.empty:
        logging.info('no changes to apply')
    else:
        logging.info('applying status changes')
        print(df)
        garjus.set_task_statuses(df)

    # TODO: complete job information from slurm, for now we just 
    # want to know about open jobs

    # Get updates from XNAT (for those no longer in dax queue) to
    # get complete or failed status
    assessors = garjus.assessors()
    df = _get_xnat_changes(gqueue, assessors)

    # Apply changes
    if df.empty:
        logging.info('no changes to apply')
    else:
        logging.info('applying status changes')
        print(df)
        garjus.set_task_statuses(df)
