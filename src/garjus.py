"""main garjus class.

Interactions with XNAT and Garjus REDCap should be via the main Garjus class.
Anything outside this class should refer to scans, assessors, issues, etc.

"""
import pathlib
from typing import Optional
import logging
import json
from datetime import datetime
import glob
import os
import tempfile
import shutil

import pandas as pd
from redcap import Project, RedcapError
from pyxnat import Interface
from requests.exceptions import ConnectionError

from .subjects import load_subjects
from . import utils_redcap
from . import utils_xnat
from . import utils_dcm2nii
from .progress import update as update_progress, make_project_report
from .compare import make_double_report, update as update_compare
from .stats import update as update_stats
from .automations import update as update_automations
from .issues import update as update_issues
from .import_dicom import import_dicom_zip, import_dicom_url, import_dicom_dir
from .dictionary import COLUMNS, PROCLIB, STATLIB, ACTIVITY_RENAME, PROCESSING_RENAME, ISSUES_RENAME, TASKS_RENAME, ANALYSES_RENAME
from .tasks import update as update_tasks
from .analyses import update as update_analyses


# TODO: export session/scan table with matching of vuiis id, subject id, and a
# column for NDA upload yes/no

# TODO: allow a date range filter in activity

# def scan_inventory():
# this will replace make_scan_table and be used by auto_archive
# as well as nda scripts, and progress exports to zip

# TODO: def import_stats(self):
# rather than source_stats from the outside, we call import_stats to tell
# garjus to go look in xnat (or wherever) to get new stats

# TODO: 
# setting for each automation for user@host to limit where some tasks can run
# like dcm2niix, edat_convert, build jobs, etc.


class Garjus:
    """
    Handles data in xnat and redcap.

    Parameters:
        redcap_project (redcap.Project): A REDCap project instance.
        xnat_interface (pyxnat.Interface): A PyXNAT interface.

    Attributes:
        redcap_project (redcap.Project): The REDCap project instance.
        xnat_interface (pyxnat.Interface): The PyXNAT interface.
    """

    def __init__(
        self,
        redcap_project: Project=None,
        xnat_interface: Interface=None
    ):
        """Initialize garjus."""
        self._disconnect_xnat = False
        self._rc = (redcap_project or self._default_redcap())

        if xnat_interface:
            self._xnat = xnat_interface
        else:
            self._xnat = self._default_xnat()
            self._disconnect_xnat = True

        self.scan_uri = utils_xnat.SCAN_URI
        self.assr_uri = utils_xnat.ASSR_URI
        self.sgp_uri = utils_xnat.SGP_URI
        self.scan_rename = utils_xnat.SCAN_RENAME
        self.assr_rename = utils_xnat.ASSR_RENAME
        self.sgp_rename = utils_xnat.SGP_RENAME
        self.activity_rename = ACTIVITY_RENAME
        self.issues_rename = ISSUES_RENAME
        self.processing_rename = PROCESSING_RENAME
        self.tasks_rename = TASKS_RENAME
        self.analyses_rename = ANALYSES_RENAME
        self.xsi2mod = utils_xnat.XSI2MOD
        self.max_stats = 60
        self._projects = self._load_project_names()
        self._project2stats = {}
        self._columns = self._default_column_names()
        self._yamldir = self.set_yamldir()
        self._tempdir = tempfile.mkdtemp()

    def __del__(self):
        """Close connectinons we opened."""
        if self._disconnect_xnat:
            logging.debug('disconnecting xnat')
            self._xnat.disconnect()

    @staticmethod
    def _default_xnat():
        from dax.XnatUtils import get_interface
        return get_interface()

    @staticmethod
    def _default_redcap():
        from .utils_redcap import get_main_redcap
        return get_main_redcap()

    def set_yamldir(self, yamldir=None):
        if yamldir:
            self._yamldir = yamldir
        elif os.path.isdir(os.path.expanduser('~/yaml_processors')):
            self._yamldir = os.path.expanduser('~/yaml_processors')
        else:
            # Default
            self._yamldir = '/data/mcr/centos7/dax_processors'

        return self._yamldir

    def has_dcm2niix(self):
        # check we have dcm2niix binary on the path
        return shutil.which('dcm2niix') is not None

    def activity(self, project=None, startdate=None):
        """List of activity records."""
        data = []

        _fields = [self._dfield()]
        if project:
            rec = self._rc.export_records(
                records=[project],
                forms=['activity'],
                fields=_fields)
        else:
            # All activity
            rec = self._rc.export_records(forms=['activity'], fields=_fields)

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'activity']
        for r in rec:
            d = {
                'PROJECT': r[self._dfield()],
                'STATUS': 'COMPLETE',
                'SOURCE': 'ccmutils'}
            for k, v in self.activity_rename.items():
                d[v] = r.get(k, '')

            data.append(d)

        df = pd.DataFrame(data, columns=self.column_names('activity'))

        if startdate:
            df = df[df.DATETIME > startdate]

        return df

    def add_activity(
        self,
        project=None,
        category=None,
        description=None,
        subject=None,
        event=None,
        session=None,
        scan=None,
        field=None,
        actdatetime=None,
        result=None,
    ):
        """Add an activity record."""
        if not actdatetime:
            actdatetime = datetime.now()

        # Format for REDCap
        activity_datetime = actdatetime.strftime("%Y-%m-%d %H:%M:%S")

        record = {
            self._dfield(): project,
            'activity_description': f'{description}:{result}',
            'activity_datetime': activity_datetime,
            'activity_event': event,
            'activity_field': field,
            'activity_result': 'COMPLETE',
            'activity_subject': subject,
            'activity_session': session,
            'activity_scan': scan,
            'activity_type': category,
            'redcap_repeat_instrument': 'activity',
            'redcap_repeat_instance': 'new',
            'activity_complete': '2',
        }

        # Add new record
        try:
            response = self._rc.import_records([record])
            assert 'count' in response
            logging.info('successfully created new record')
        except (ValueError, RedcapError, AssertionError) as err:
            logging.error(f'error uploading:{err}')

    def assessors(self, projects=None, proctypes=None):
        """Query XNAT for all assessors of and return list of dicts."""
        if not projects:
            projects = self.projects()

        data = self._load_assr_data(projects, proctypes)

        # Build a dataframe
        return pd.DataFrame(data, columns=self.column_names('assessors'))

    def subject_assessors(self, projects=None, proctypes=None):
        """Query XNAT for all subject assessors, return dataframe."""
        if not projects:
            projects = self.projects()

        data = self._load_sgp_data(projects, proctypes)

        # Build a dataframe
        return pd.DataFrame(data, columns=self.column_names('sgp'))

    def column_names(self, datatype):
        """Return list of colum names for this data type."""
        return self._columns.get(datatype)

    def issues(self, project=None):
        """Return the current existing issues data as list of dicts."""
        data = []

        # Get the data from redcap
        _fields = [self._dfield()]
        if project:
            # Only the specified project
            rec = self._rc.export_records(
                records=[project],
                forms=['issues'],
                fields=_fields,
            )
        else:
            # All issues
            rec = self._rc.export_records(forms=['issues'], fields=_fields)

        # Only unresolved issues
        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'issues']
        rec = [x for x in rec if str(x['issues_complete']) != '2']

        # Reformat each record
        for r in rec:
            d = {'PROJECT': r[self._dfield()], 'STATUS': 'FAIL'}
            for k, v in self.issues_rename.items():
                d[v] = r.get(k, '')

            data.append(d)

        # Finally, build a dataframe
        return pd.DataFrame(data, columns=self.column_names('issues'))

    def tasks(self, download=False):
        """List of task records."""
        data = []

        rec = self._rc.export_records(
            forms=['taskqueue'],
            fields=[self._dfield()])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'taskqueue']
        for r in rec:
            d = {
                'PROJECT': r[self._dfield()],
                'ID': r['redcap_repeat_instance']
            }
            for k, v in self.tasks_rename.items():
                d[v] = r.get(k, '')

            data.append(d)

        return pd.DataFrame(data, columns=self.column_names('task'))

    def save_task_yaml(self, project, task_id, yaml_dir):
        return utils_redcap.download_named_file(
            self._rc,
            project,
            'task_yamlupload',
            yaml_dir,
            repeat_id=task_id)

    def set_task_statuses(self, tasks):
        records = []

        # Build list of task updates
        for i, t in tasks.iterrows():
            r = {
                self._dfield(): t['PROJECT'],
                'redcap_repeat_instance': t['ID'],
                'redcap_repeat_instrument': 'taskqueue',
                #'taskqueue_complete': 1,
                'task_status': t['STATUS']
            }
            if t['STATUS'] == 'COMPLETE':
                r['taskqueue_complete'] = '2'

            records.append(r)

        # Apply the updates in one call
        try:
            response = self._rc.import_records(records)
            assert 'count' in response
            logging.info('task statuses successfully updated')
        except AssertionError as err:
            logging.error(f'failed to set task statuses:{err}')

    def set_task_status(self, project, task_id, status):
        record = {
            self._dfield(): project,
            'redcap_repeat_instance': task_id,
            'redcap_repeat_instrument': 'taskqueue',
            'task_status': status,
        }

        if status == 'COMPLETE':
            record['taskqueue_complete'] = '2'
        elif status == 'JOB_FAILED':
            record['taskqueue_complete'] = '0'

        try:
            response = self._rc.import_records([record])
            assert 'count' in response
            logging.info('task status successfully updated')
        except AssertionError as err:
            logging.error(f'failed to set task status:{err}')

    def delete_old_issues(self, projects=None, days=7):
        old_issues = []

        # Get the data from redcap
        _fields = [self._dfield()]
        if projects:
            # Only the specified project
            rec = self._rc.export_records(
                records=projects,
                forms=['issues'],
                fields=_fields,
            )
        else:
            # All issues
            rec = self._rc.export_records(forms=['issues'], fields=_fields)

        # Only resolved issues
        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'issues']
        rec = [x for x in rec if str(x['issues_complete']) == '2']

        # Find old issues
        for r in rec:
            # Find how many days old the record is
            issue_date = r['issue_closedate']
            try:
                issue_date = datetime.strptime(issue_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                issue_date = datetime.strptime(issue_date, '%Y-%m-%d')

            # Determine how many days old
            days_old = (datetime.now() - issue_date).days

            # Append to list if more than requested days
            if days_old >= days:
                _main = r[self._dfield()],
                _id = r['redcap_repeat_instance']
                logging.debug(f'{_main}:{_id}:{days_old} days old')
                old_issues.append(r)

        # Apply delete to list of old issues
        self.delete_issues(old_issues)

    def import_dicom(self, src, dst):
        """Import dicom source to destination."""
        logging.info(f'uploading from:{src}')

        (proj, subj, sess) = dst.split('/')
        logging.info(f'uploading to:{proj},{subj},{sess}')

        if src.endswith('.zip'):
            import_dicom_zip(self, src, proj, subj, sess)
        elif src.startswith('http'):
            # e.g. gstudy link
            import_dicom_url(self, src, proj, subj, sess)
        elif os.path.isdir(src):
            import_dicom_dir(self, src, proj, subj, sess)
        else:
            self.import_dicom_xnat(src, proj, subj, sess)

        logging.info(f'adding activity:{src}')
        self.add_activity(
            project=proj,
            category='import_dicom',
            description=src,
            subject=subj,
            session=sess,
            result='COMPLETE')

    def copy_sess(self, src, dst):
        """Copy dicom source to destination."""
        logging.info(f'copy from:{src}')
        logging.info(f'copy to:{dst}')
        (s_proj, s_subj, s_sess) = src.split('/')
        (d_proj, d_subj, d_sess) = dst.split('/')
        self.copy_session(s_proj, s_subj, s_sess, d_proj, d_subj, d_sess)
        self.add_activity(
            project=d_proj,
            category='copy_sess',
            description=src,
            subject=d_subj,
            session=d_sess,
            result='COMPLETE')

    def scans(self, projects=None, scantypes=None, modalities='MR'):
        """Query XNAT for all scans and return a dictionary of scan info."""
        if not projects:
            projects = self.projects()

        data = self._load_scan_data(projects, scantypes, modalities)

        # Return as dataframe
        return pd.DataFrame(data, columns=self.column_names('scans'))


    def phantoms(self, project):
        """Query XNAT for all scans and return a dictionary of scan info."""
        phan_project = self.project_setting(project, 'phanproject')

        if phan_project:
            data = self._load_scan_data([phan_project], scantypes=None)
        else:
            data = []

        # Return as dataframe
        return pd.DataFrame(data, columns=self.column_names('scans'))

    def session_labels(self, project):
        """Return list of session labels in the archive for project."""
        uri = f'/REST/experiments?columns=label,modality&project={project}'
        result = self._get_result(uri)
        label_list = [x['label'] for x in result]
        return label_list

    def session_source_labels(self, project):
        """Return list of source session IDs for project."""
        tag = 'dcmPatientId'
        uri = '/REST/projects/{0}/experiments?columns=label,xnat:imagesessiondata/{1}'
        uri = uri.format(project, tag)
        result = self._get_result(uri)
        srcid_list = [x[tag].split('_', 1)[1] for x in result if '_' in x[tag]]
        return srcid_list

    def sites(self, project):
        """List of site records."""
        return self._rc.export_records(records=[project], forms=['sites'])

    def _load_project_names(self):
        _records = self._rc.export_records(fields=[self._rc.def_field])
        return [x[self._rc.def_field] for x in _records]

    def _default_column_names(self):
        return COLUMNS

    def _stats_redcap(self, project):
        if project not in self._project2stats:
            # get the project ID for the stats redcap for this project
            _fields = [self._dfield(), 'project_stats']
            rec = self._rc.export_records(records=[project], fields=_fields)
            rec = [x for x in rec if x[self._dfield()] == project][0]
            redcap_id = rec['project_stats']
            self._project2stats[project] = utils_redcap.get_redcap(redcap_id)

        return self._project2stats[project]

    def analyses(self, project, download=False):
        """Return analyses."""
        data = []

        rec = self._rc.export_records(
            records=[project],
            forms=['analyses'],
            fields=[self._dfield()])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'analyses']
        for r in rec:
            # Initialize record with project
            d = {'PROJECT': r[self._dfield()]}

            # Get renamed variables
            for k, v in self.analyses_rename.items():
                d[v] = r.get(k, '')

            # Finally, add to our list
            data.append(d)

        return pd.DataFrame(data, columns=self.column_names('analyses'))

    def stats(self, project, proctypes=None):
        """Return all stats for project, filtered by proctypes."""
        try:
            """Get the stats data from REDCap."""
            statsrc = self._stats_redcap(project)
        except:
            return pd.DataFrame(columns=['stats_assr'])

        rec = statsrc.export_records(forms=['stats'])

        # Filter out FS6 if found
        rec = [x for x in rec if 'FS6_v1' not in x['stats_assr']]

        # Make a dataframe of columns we need
        df = pd.DataFrame(
            rec,
            columns=['stats_assr', 'stats_name', 'stats_value'])

        # print(df[df.duplicated(['stats_assr', 'stats_name'], keep=False)])
        # TODO: df = df.drop_duplicates()

        # Pivot to row per assessor, col per stats_name, values as stats_value
        dfp = pd.pivot(
            df, index='stats_assr', values='stats_value', columns='stats_name')
        dfp = dfp.reset_index()

        return dfp

    def stats_assessors(self, project, proctypes=None):
        """Get list of assessors alread in stats archive."""
        statsrc = self._stats_redcap(project)

        _records = statsrc.export_records(fields=['stats_assr'])
        return list(set([x['stats_assr'] for x in _records]))

    def projects(self):
        """Get list of projects."""
        return self._projects


    def subjects(self, project):
        """Return subjects for project."""

        return load_subjects(self, project)

    def stattypes(self, project):
        """Get list of projects stat types."""
        types = []

        # Start with defaults
        types = self._default_stattypes()

        # Append others
        protocols = self.processing_protocols(project)
        for i, row in protocols.iterrows():
            ptype = row['TYPE']
            if ptype not in types:
                logging.debug(f'appending proctype:{ptype}')
                types.append(ptype)

        types = [x for x in types if x not in ['fmri_rest_v2', 'fmri_roi_v1', 'struct_preproc_noflair_v1', 'fmri_nback_v2']]

        return types

    def _get_proctype(self, procfile):
        # Get just the filename without the directory path
        tmp = os.path.basename(procfile)

        # Split on periods and grab the 4th value from right, 
        # thus allowing periods in the main processor name
        return tmp.rsplit('.')[-4]

    def all_scantypes(self):
        """Get list of scan types."""
        types = []

        rec = self._rc.export_records(
            forms=['scanning'],
            export_checkbox_labels=True,
            raw_or_label='label')

        for r in rec:
            for k, v in r.items():
                # Append types for this scanning record
                if v and k.startswith('scanning_scantypes'):
                    types.append(v)

        # Make the lists unique
        return list(set(types))

    def scantypes(self, project):
        """Get list of scan types."""
        types = []

        # Get the scan types from scanning forms
        rec = self._rc.export_records(
            forms=['scanning'],
            records=[project],
            export_checkbox_labels=True,
            raw_or_label='label')

        for r in rec:
            for k, v in r.items():
                # Append types for this scanning record
                if v and k.startswith('scanning_scantypes'):
                    types.append(v)

        # Make the lists unique
        types = list(set((types)))

        if not types:
            types = self._default_scantypes()

        return types

    def all_proctypes(self):
        """Get list of project proc types."""
        types = self._default_proctypes()

        rec = self._rc.export_records(forms=['processing'])

        for r in rec:
            if r['processor_file'] == 'CUSTOM':
                dtype = self._get_proctype(r['processor_custom'])
            else:
                dtype = self._get_proctype(r['processor_file'])

            # Finally, add to our list
            types.append(dtype)

        return list(set(types))

    def proctypes(self, project):
        """Get list of project proc types."""
        types = []

        # Start with defaults
        types = self._default_proctypes()

        # Append others
        protocols = self.processing_protocols(project)
        for i, row in protocols.iterrows():
            ptype = row['TYPE']
            if ptype not in types:
                logging.debug(f'appending proctype:{ptype}')
                types.append(ptype)

        return types

    def _load_scan_data(self, projects=None, scantypes=None, modalities=None):
        """Get scan info from XNAT as list of dicts."""
        scans = []
        uri = self.scan_uri

        if projects:
            uri += f'&project={",".join(projects)}'

        result = self._get_result(uri)

        # Change from one row per resource to one row per scan
        scans = {}
        for r in result:
            k = (r['project'], r['session_label'], r['xnat:imagescandata/id'])
            if k in scans.keys():
                # Append to list of resources
                _resource = r['xnat:imagescandata/file/label']
                scans[k]['RESOURCES'] += ',' + _resource
            else:
                scans[k] = self._scan_info(r)

        # Get just the values in a list
        scans = list(scans.values())

        # Filter by scan type
        if scantypes:
            scans = [x for x in scans if x['SCANTYPE'] in scantypes]

        return scans

    def _load_assr_data(self, projects=None, proctypes=None):
        """Get assessor info from XNAT as list of dicts."""
        assessors = []
        uri = self.assr_uri

        if projects:
            uri += f'&project={",".join(projects)}'

        result = self._get_result(uri)

        for r in result:
            assessors.append(self._assessor_info(r))

        # Filter by type
        if proctypes:
            assessors = [x for x in assessors if x['PROCTYPE'] in proctypes]

        return assessors

    def _load_sgp_data(self, projects=None, proctypes=None):
        """Get assessor info from XNAT as list of dicts."""
        assessors = []
        uri = self.sgp_uri

        if projects:
            uri += f'&project={",".join(projects)}'

        result = self._get_result(uri)

        for r in result:
            assessors.append(self._sgp_info(r))

        # Filter by type
        if proctypes:
            assessors = [x for x in assessors if x['PROCTYPE'] in proctypes]

        return assessors

    def _get_result(self, uri):
        """Get result of xnat query."""
        logging.debug(uri)
        json_data = json.loads(self._xnat._exec(uri, 'GET'), strict=False)
        result = json_data['ResultSet']['Result']
        return result

    def _scan_info(self, record):
        """Get scan info."""
        info = {}

        for k, v in self.scan_rename.items():
            info[v] = record[k]

        # set_modality
        info['MODALITY'] = self.xsi2mod.get(info['XSITYPE'], 'UNK')

        # Get the full path
        _p = '/projects/{0}/subjects/{1}/experiments/{2}/scans/{3}'.format(
            info['PROJECT'],
            info['SUBJECT'],
            info['SESSION'],
            info['SCANID'])
        info['full_path'] = _p

        return info

    def _assessor_info(self, record):
        """Get assessor info."""
        info = {}

        for k, v in self.assr_rename.items():
            info[v] = record[k]

        # Decode inputs into list
        info['INPUTS'] = utils_xnat.decode_inputs(info['INPUTS'])

        # Get the full path
        _p = '/projects/{0}/subjects/{1}/experiments/{2}/assessors/{3}'.format(
            info['PROJECT'],
            info['SUBJECT'],
            info['SESSION'],
            info['ASSR'])
        info['full_path'] = _p

        # set_modality
        info['MODALITY'] = self.xsi2mod.get(info['XSITYPE'], 'UNK')

        return info

    def _sgp_info(self, record):
        """Get subject assessor info."""
        info = {}

        # Copy with new var names
        for k, v in self.sgp_rename.items():
            info[v] = record[k]

        # Decode inputs into list
        info['INPUTS'] = utils_xnat.decode_inputs(info['INPUTS'])

        # Get the full path
        _p = '/projects/{0}/subjects/{1}/assessors/{2}'.format(
            info['PROJECT'],
            info['SUBJECT'],
            info['ASSR'])
        info['full_path'] = _p

        return info

    def _dfield(self):
        """Name of redcap filed that stores project name."""
        return self._rc.def_field

    def progress_reports(self, projects=None):
        """List of progress records."""
        rec = self._rc.export_records(
            forms=['progress'],
            fields=[self._dfield()])

        if projects:
            rec = [x for x in rec if x[self._dfield()] in projects]

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'progress']
        rec = [x for x in rec if str(x['progress_complete']) == '2']
        return rec

    def double_reports(self, projects=None):
        """List of progress records."""
        rec = self._rc.export_records(
            forms=['double'],
            fields=[self._dfield()])

        if projects:
            rec = [x for x in rec if x[self._dfield()] in projects]

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'double']
        rec = [x for x in rec if str(x['double_complete']) == '2']
        return rec

    def processing_protocols(self, project, download=False):
        """Return processing protocols."""
        data = []

        rec = self._rc.export_records(
            records=[project],
            forms=['processing'],
            fields=[self._dfield()])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'processing']
        for r in rec:
            # Initialize record with project
            d = {'PROJECT': r[self._dfield()]}

            # Find the yaml file
            if r['processor_yamlupload'] and download:
                filename = os.path.join(self._tempdir, r['processor_yamlupload'])
                filepath = utils_redcap.download_file(
                    self._rc,
                    project,
                    'processor_yamlupload',
                    filename,
                    repeat_id=r['redcap_repeat_instance'])
            elif r['processor_file'] == 'CUSTOM':
                filepath = r['processor_custom']
            else:
                filepath = r['processor_file']

            if not os.path.isabs(filepath):
                # Prepend lib location
                filepath = os.path.join(self._yamldir, filepath)

            if not os.path.isfile(filepath):
                logging.debug(f'file not found:{filepath}, download={download}')
                continue

            # Get renamed variables
            for k, v in self.processing_rename.items():
                d[v] = r.get(k, '')

            d['FILE'] = filepath
            d['TYPE'] = self._get_proctype(d['FILE'])

            # Finally, add to our list
            data.append(d)

        return pd.DataFrame(data, columns=self.column_names('processing'))

    def processing_library(self):
        """Return processing library."""
        return PROCLIB

    def stats_library(self):
        """Return stats library."""
        return STATLIB

    def update(self, projects=None, choices=None):
        """Update projects."""
        if not projects:
            projects = self._projects

        if not choices:
            choices = ['automations', 'stats', 'tasks', 'issues',  'progress', 'compare']

        logging.info(f'updating projects:{projects}:{choices}')

        if 'automations' in choices:
            logging.info('updating automations')
            update_automations(self, projects)

        if 'issues' in choices:
            logging.info('updating issues')
            update_issues(self, projects)
            logging.info('deleting old issues')
            self.delete_old_issues(projects)

        if 'stats' in choices:
            # Only run on intersect of specified projects and projects with
            # stats, such that if the list is empty, nothing will run
            logging.info('updating stats')
            _projects = [x for x in projects if x in self.stats_projects()]
            update_stats(self, _projects)

        if 'progress' in choices:
            # confirm each project has report for current month with PDF & zip
            logging.info('updating progress')
            update_progress(self, projects)

        if 'compare' in choices:
            # confirm each project has report for current month
            logging.info('updating compare')
            update_compare(self, projects)

        if 'tasks' in choices:
            logging.info('updating tasks')
            update_tasks(self, projects)

        if 'analyses' in choices:
            logging.info('updating analyses')
            update_analyses(self, projects)

    def report(self, project):
        """Create a PDF report."""
        pdf_file = f'{project}_report.pdf'

        if os.path.exists(pdf_file):
            logging.info(f'{pdf_file} exists, delete or rename.')
            return

        logging.info(f'writing report to file:{pdf_file}.')
        make_project_report(self, project, pdf_file)

    def compare(self, project):
        """Create a PDF report of Double Entry Comparison."""
        pdf_file = f'{project}_double.pdf'
        excel_file = f'{project}_double.xlsx'

        if os.path.exists(pdf_file):
            logging.info(f'{pdf_file} exists, delete or rename.')
            return

        if os.path.exists(excel_file):
            logging.info(f'{excel_file} exists, delete or rename.')
            return

        logging.info(f'writing report to file:{pdf_file},{excel_file}.')
        # Get the projects to compare
        proj_primary = self.primary(project)
        proj_secondary = self.secondary(project)
        make_double_report(proj_primary, proj_secondary, pdf_file, excel_file)

    def stats_projects(self):
        """List of projects that have stats, checks for a stats project ID."""
        _fields = [self._dfield(), 'project_stats']
        rec = self._rc.export_records(fields=_fields)
        return [x[self._dfield()] for x in rec if x['project_stats']]

    def add_task(self, project, assr, inputlist, var2val, walltime, memreq, yamlfile, userinputs):
        """Add a new task record ."""

        # Convert to string for storing
        var2val = json.dumps(var2val)
        inputlist = json.dumps(inputlist)

        # Try to match existing record
        task_id = self.assessor_task_id(project, assr)

        if os.path.dirname(yamlfile) != self._yamldir:
            task_yamlfile = 'CUSTOM'
        else:
            task_yamlfile = os.path.basename(yamlfile)
 
        if task_id:
            # Update existing record
            try:
                record = {
                    'main_name': project,
                    'redcap_repeat_instrument': 'taskqueue',
                    'redcap_repeat_instance': task_id,
                    'task_status': 'JOB_QUEUED',
                    'task_inputlist': inputlist,
                    'task_var2val': var2val,
                    'task_walltime': walltime,
                    'task_memreq': memreq,
                    'task_yamlfile': task_yamlfile,
                    'task_userinputs': userinputs,
                    'task_timeused': '',
                    'task_memused': '',
                }
                response = self._rc.import_records([record])
                assert 'count' in response
                logging.info('successfully created new record')
            except AssertionError as err:
                logging.error(f'upload failed:{err}')
                return
        else:
            # Create a new record
            try:
                record = {
                    'main_name': project,
                    'redcap_repeat_instrument': 'taskqueue',
                    'redcap_repeat_instance': 'new',
                    'task_assessor': assr,
                    'task_status': 'JOB_QUEUED',
                    'task_inputlist': inputlist,
                    'task_var2val': var2val,
                    'task_walltime': walltime,
                    'task_memreq': memreq,
                    'task_yamlfile': task_yamlfile,
                    'task_userinputs': userinputs,
                }
                response = self._rc.import_records([record])
                assert 'count' in response
                logging.info('successfully created new record')
            except AssertionError as err:
                logging.error(f'upload failed:{err}')
                return

        # If the file is not in yaml dir, we need to upload it to the task
        if task_yamlfile == 'CUSTOM':
            logging.info(f'yaml not in shared library, uploading to task')
            if not task_id:
                # Try to match existing record
                task_id = self.assessor_task_id(project, assr)

            logging.info(f'uploading file:{yamlfile}')
            utils_redcap.upload_file(
                self._rc,
                project,
                'task_yamlupload',
                yamlfile,
                repeat_id=task_id)

    def assessor_task_id(self, project, assessor):
        task_id = None

        rec = self._rc.export_records(
            forms=['taskqueue'],
            records=[project],
            fields=[self._dfield(), 'task_assessor'])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'taskqueue']
        rec = [x for x in rec if x['task_assessor'] == assessor]

        if len(rec) > 1:
            logging.warn(f'duplicate tasks for assessor, not good:{assessor}')
            task_id = rec[0]['redcap_repeat_instance']
        elif len(rec) == 1:
            task_id = rec[0]['redcap_repeat_instance']

        return task_id


    def add_progress(self, project, prog_name, prog_date, prog_pdf, prog_zip):
        """Add a progress record with PDF and Zip at dated and named."""
        # Format for REDCap
        progress_datetime = prog_date.strftime("%Y-%m-%d %H:%M:%S")

        # Add new record
        try:
            record = {
                'progress_datetime': progress_datetime,
                'main_name': project,
                'redcap_repeat_instrument': 'progress',
                'redcap_repeat_instance': 'new',
                'progress_name': prog_name,
                'progress_complete': '2',
            }
            response = self._rc.import_records([record])
            assert 'count' in response
            logging.info('successfully created new record')

            # Determine the new record id
            logging.debug('locating new record')
            _ids = utils_redcap.match_repeat(
                self._rc,
                project,
                'progress',
                'progress_datetime',
                progress_datetime)
            repeat_id = _ids[-1]

            # Upload output files
            logging.debug(f'uploading files to:{repeat_id}')
            utils_redcap.upload_file(
                self._rc,
                project,
                'progress_pdf',
                prog_pdf,
                repeat_id=repeat_id)
            utils_redcap.upload_file(
                self._rc,
                project,
                'progress_zip',
                prog_zip,
                repeat_id=repeat_id)

        except AssertionError as err:
            logging.error(f'upload failed:{err}')
        except (ValueError, RedcapError) as err:
            logging.error(f'error uploading:{err}')

    def add_double(self, project, comp_name, comp_date, comp_pdf, comp_excel):
        """Add a compare record with PDF and Excel at dated and named."""

        # Format for REDCap
        compare_datetime = comp_date.strftime("%Y-%m-%d %H:%M:%S")

        # Add new record
        try:
            record = {
                'double_datetime': compare_datetime,
                'main_name': project,
                'redcap_repeat_instrument': 'double',
                'redcap_repeat_instance': 'new',
                'double_name': comp_name,
                'double_complete': '2',
            }
            response = self._rc.import_records([record])
            assert 'count' in response
            logging.info('successfully created new record')

            # Determine the new record id
            logging.debug('locating new record')
            _ids = utils_redcap.match_repeat(
                self._rc,
                project,
                'double',
                'double_datetime',
                compare_datetime)
            repeat_id = _ids[-1]

            # Upload output files
            logging.debug(f'uploading files to:{repeat_id}')
            utils_redcap.upload_file(
                self._rc,
                project,
                'double_resultspdf',
                comp_pdf,
                repeat_id=repeat_id)
            utils_redcap.upload_file(
                self._rc,
                project,
                'double_resultsfile',
                comp_excel,
                repeat_id=repeat_id)

        except AssertionError as err:
            logging.error(f'upload failed:{err}')
        except (ValueError, RedcapError) as err:
            logging.error(f'error uploading:{err}')

    def get_source_stats(self, project, subject, session, assessor, stats_dir):
        """Download stats files to directory."""
        resource = 'STATS'

        xnat_resource = self._xnat.select_assessor_resource(
            project,
            subject,
            session,
            assessor,
            resource)

        xnat_resource.get(stats_dir, extract=True)

        return f'{stats_dir}/STATS'

    def set_stats(self, project, subject, session, assessor, data):
        """Upload stats to redcap."""
        if len(data.keys()) > self.max_stats:
            logging.debug('found more than 50 stats:too many, specify subset')
            return

        # Create list of stat records
        rec = [{'stats_name': k, 'stats_value': v} for k, v in data.items()]

        # Build out the records
        for r in rec:
            r['subject_id'] = subject
            r['stats_assr'] = assessor
            r['redcap_repeat_instrument'] = 'stats'
            r['redcap_repeat_instance'] = 'new'
            r['stats_complete'] = 2

        # Now upload
        logging.debug('uploading to redcap')
        statsrc = self._stats_redcap(project)
        try:
            logging.debug('importing records')
            response = statsrc.import_records(rec)
            assert 'count' in response
            logging.debug('successfully uploaded')
        except AssertionError as err:
            logging.error(f'upload failed:{err}')
        except ConnectionError as err:
            logging.error(err)
            logging.info('wait a minute')
            import time
            time.sleep(60)

    def delete_stats(self, project, subject, session, assessor):
        """Get all the repeat instances for this assessor."""
        # Make the payload
        # payload = {
        # 'action': 'delete',
        # 'returnFormat': 'json',
        # 'content': 'record',
        # 'format': 'json',
        # 'instrument': 'stats',
        # 'token': str(rc.token),
        # 'records[0]': str(record_id),
        # 'repeat_instance': str(repeat_id),
        # }

        # Call delete
        # result = rc._call_api(payload, 'del_record')

        pass

    def project_setting(self, project, setting):
        """Return the value of the setting for this project."""
        records = self._rc.export_records(records=[project], forms=['main'])
        if not records:
            return None

        # First try "project" then try "main"
        rec = records[0]
        return rec.get(f'project_{setting}', rec.get(f'main_{setting}', None))

    def etl_automations(self, project):
        """Get ETL automation records."""
        etl_autos = []
        auto_names = self.etl_automation_choices()
        rec = self._rc.export_records(records=[project], forms=['main'])[0]

        # Determine which automations we want to run
        for a in auto_names:
            if rec.get(f'main_etlautos___{a}', '') == '1':
                etl_autos.append(a)

        return etl_autos

    def etl_automation_choices(self):
        """Get the names of the automations, checkboxes in REDCap."""
        names = None

        for x in self._rc.metadata:
            # dcm2niix, dcm2niix | xnat_auto_archive, xnat_auto_archive
            if x['field_name'] == 'main_etlautos':
                names = x['select_choices_or_calculations']
                names = [x for x in names.split('|')]
                names = [n.split(',')[0].strip() for n in names]

        return names

    def scan_automation_choices(self):
        """Get the names of the automations, checkboxes in REDCap."""
        names = None

        for x in self._rc.metadata:
            # dcm2niix, dcm2niix | xnat_auto_archive, xnat_auto_archive
            if x['field_name'] == 'main_scanautos':
                names = x['select_choices_or_calculations']
                names = [x for x in names.split('|')]
                names = [n.split(',')[0].strip() for n in names]

        return names

    def scan_automations(self, project):
        """Get scanning automation records."""
        scan_autos = []
        auto_names = self.scan_automation_choices()
        rec = self._rc.export_records(records=[project], forms=['main'])[0]

# check these settings before allowing automations for xnat
# check that projects exist on XNAT
#        if not xnat.select.project(src_project_name).exists():
#            logger.error(f'source project not on XNAT:{src_project_name}')
#            # TODO: create an issue?
#            return
#
#        # check that projects exist on XNAT
#        if not xnat.select.project(dst_project_name).exists():
#            logger.error(f'destination project found:{dst_project_name}')
#            # TODO: create an issue?
#            return

        # Determine what scan autos we want to run
        for a in auto_names:
            if rec.get(f'main_scanautos___{a}', '') == '1':
                scan_autos.append(a)

        return scan_autos

    def edat_protocols(self, project):
        """Return list of edat protocol records."""
        return self._rc.export_records(records=[project], forms=['edat'])

    def scanning_protocols(self, project):
        """Return list of scanning protocol records."""
        return self._rc.export_records(records=[project], forms=['scanning'])

    def add_issues(self, issues):
        """Add list of issues."""
        records = []
        issue_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for i in issues:
            records.append({
                self._dfield(): i['project'],
                'issue_description': i['description'],
                'issue_date': issue_datetime,
                'issue_subject': i.get('subject', None),
                'issue_session': i.get('session', None),
                'issue_scan': i.get('scan', None),
                'issue_event': i.get('event', None),
                'issue_field': i.get('field', None),
                'issue_type': i.get('category', None),
                'redcap_repeat_instrument': 'issues',
                'redcap_repeat_instance': 'new',
            })

        try:
            logging.debug(records)
            response = self._rc.import_records(records)
            assert 'count' in response
            logging.debug('issues successfully uploaded')
        except AssertionError as err:
            logging.error(f'issues upload failed:{err}')

    def add_issue(
        self,
        description,
        project,
        event=None,
        subject=None,
        scan=None,
        session=None,
        field=None,
        category=None
    ):
        """Add a new issue."""
        # Format for REDCap
        issue_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        record = {
            self._dfield(): project,
            'issue_description': description,
            'issue_date': issue_datetime,
            'issue_subject': subject,
            'issue_session': session,
            'issue_scan': scan,
            'issue_event': event,
            'issue_field': field,
            'issue_type': category,
            'redcap_repeat_instrument': 'issues',
            'redcap_repeat_instance': 'new',
        }

        # Add new record
        try:
            response = self._rc.import_records([record])
            assert 'count' in response
            logging.info('successfully created new record')
        except (ValueError, RedcapError, AssertionError) as err:
            logging.error(f'error uploading:{err}')

    def _default_proctypes(self):
        """Get list of default processing types."""
        return [
            'dtiQA_synb0_v7', 'biscuit_fs_v2', 'FS7_v1', 'LST_v1',
            'SAMSEG_v1', 'fmriqa_v4', 'slant_gpu_v1']

    def _default_scantypes(self):
        """Get list of default scan types."""
        return ['T1', 'CTAC', 'FLAIR']

    def _default_stattypes(self):
        """Return list of default stats types."""
        return [
            'FS7_v1', 'LST_v1', 'AMYVIDQA_v1',
            'BrainAgeGap_v2', 'FS7HPCAMG_v1',
            'SAMSEG_v1',  'fmriqa_v4']

    def primary(self, project):
        """Connect to the primary redcap for this project."""
        primary_redcap = None
        project_id = self.project_setting(project, 'primary')
        if not project_id:
            logging.debug(f'no primary project id found:{project}')
            return None

        try:
            primary_redcap = utils_redcap.get_redcap(project_id)
        except Exception as err:
            logging.info(f'failed to load primary redcap:{project}:{err}')
            primary_redcap = None

        return primary_redcap


    def secondary(self, project):
        """Connect to the secondary redcap for this project."""
        secondary_redcap = None
        project_id = self.project_setting(project, 'secondary')
        if not project_id:
            logging.debug(f'no secondary project id found:{project}')
            return None

        try:
            secondary_redcap = utils_redcap.get_redcap(project_id)
        except Exception as err:
            logging.info(f'failed to load secondary redcap:{project}:{err}')
            secondary_redcap = None

        return secondary_redcap


    def alternate(self, project_id):
        """Connect to the alternate redcap with this ID."""
        alt_redcap = None

        try:
            alt_redcap = utils_redcap.get_redcap(project_id)
        except Exception as err:
            logging.info(f'failed to load alternate redcap:{project_id}:{err}')
            alt_redcap = None

        return alt_redcap

    def xnat(self):
        """Get the xnat for this garjus."""
        return self._xnat

    def copy_session(
        self,
        src_proj,
        src_subj,
        src_sess,
        dst_proj,
        dst_subj,
        dst_sess
    ):
        """Copy scanning/imaging session from source to destination."""
        src_obj = self._xnat.select_session(src_proj, src_subj, src_sess)
        dst_obj = self._xnat.select_session(dst_proj, dst_subj, dst_sess)
        utils_xnat.copy_session(src_obj, dst_obj)

    def source_project_exists(self, project):
        """True if this project exist in the source projects."""
        return self._xnat.select.project(project).exists()

    def project_exists(self, project):
        """True if this this project exists."""
        redcap_exists = (project in self.projects())
        xnat_exists = self._xnat.select.project(project).exists()
        return redcap_exists and xnat_exists

    def close_issues(self, issues):
        """Close specified issues, set to complete in REDCap."""
        records = []
        issue_closedate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for i in issues:
            records.append({
                self._dfield(): i['project'],
                'redcap_repeat_instance': i['id'],
                'issue_closedate': issue_closedate,
                'redcap_repeat_instrument': 'issues',
                'issues_complete': 2,
            })

        try:
            response = self._rc.import_records(records)
            assert 'count' in response
            logging.info('issues successfully completed')
        except AssertionError as err:
            logging.error(f'failed to set issues to complete:{err}')

    def delete_issues(self, issues):
        """Delete specified issues, delete in REDCap."""
        try:
            for i in issues:
                _main = i[self._dfield()],
                _id = i['redcap_repeat_instance']
                logging.info(f'deleting:issue:{_main}:{_id}')
                # https://redcap.vanderbilt.edu/api/help/?content=del_records
                _payload = {
                    'action': 'delete',
                    'returnFormat': 'json',
                    'records[0]': _main,
                    'instrument': 'issues',
                    'repeat_instance': _id,
                    'content': 'record',
                    'token': self._rc.token,
                    'format': 'json'}

                self._rc._call_api(_payload, 'del_record')
        except Exception as err:
            logging.error(f'failed to delete records:{err}')

    def rename_dicom(self, in_dir, out_dir):
        """Sort DICOM folder into scans."""
        utils_dcm2nii.rename_dicom(in_dir, out_dir)

    def _load_json_info(self, jsonfile):
        with open(jsonfile) as f:
            data = json.load(f, strict=False)

        return {
            'modality': data.get('Modality', None),
            'date': data.get('AcquisitionDateTime', None)[:10],
            'tracer': data.get('Radiopharmaceutical', None),
        }

    def _upload_scan(self, dicomdir, scan_object):
        nifti_list = []
        bval_path = ''
        bvec_path = ''
        json_path = ''

        # check that it hasn't been converted yet
        nifti_count = len(glob.glob(os.path.join(dicomdir, '*.nii.gz')))
        if nifti_count > 0:
            logging.info(f'nifti exists:{dicomdir}')
            return None

        # convert
        niftis = utils_dcm2nii.dicom2nifti(dicomdir)
        if not niftis:
            logging.info(f'nothing converted:{dicomdir}')
            return None

        # if session needs to be created, get the attributes from the scan json
        jsonfile = glob.glob(os.path.join(dicomdir, '*.json'))[0]

        # load json data from file
        scan_info = self._load_json_info(jsonfile)
        scan_modality = scan_info['modality']
        scan_date = scan_info['date']
        scan_tracer = scan_info['tracer']

        if scan_modality == 'MR':
            sess_datatype = 'xnat:mrSessionData'
            scan_datatype = 'xnat:mrScanData'
        elif scan_modality == 'PT':
            sess_datatype = 'xnat:petSessionData'
            scan_datatype = 'xnat:petScanData'
        elif scan_modality == 'CT':
            sess_datatype = 'xnat:petSessionData'
            scan_datatype = 'xnat:ctScanData'
        else:
            logging.info(f'unsupported modality:{scan_modality}')
            return

        if not scan_object.parent().exists():
            # create session with date, modality
            logging.info(f'creating xnat session:type={sess_datatype}')
            scan_object.parent().create(experiments=sess_datatype)
            logging.info(f'set date={scan_date}')
            scan_object.parent().attrs.set('date', scan_date)

        scan_type = os.path.basename(niftis[0])
        scan_type = scan_type.split('_', 1)[1]
        scan_type = scan_type.rsplit('.nii', 1)[0]
        scan_attrs = {
            'series_description': scan_type,
            'type': scan_type,
            'quality': 'usable'}

        if scan_modality == 'PT' and scan_tracer:
            # Set the PET tracer name at session level
            logging.info(f'set tracer:{scan_tracer}')
            scan_object.parent().attrs.set('tracer_name', scan_tracer)

        if not scan_object.exists():
            logging.info(f'creating xnat scan:datatype={scan_datatype}')

            # make the scan
            scan_object.create(scans=scan_datatype)
            scan_object.attrs.mset(scan_attrs)

        elif scan_object.resource('DICOMZIP').exists():
            logging.info('skipping, DICOMZIP already exists')
            return

        # upload the converted files, NIFTI/JSON/BVAL/BVEC
        for fpath in glob.glob(os.path.join(dicomdir, '*')):
            if not os.path.isfile(fpath):
                continue

            if fpath.endswith('ADC.nii.gz'):
                logging.info(f'ignoring ADC NIFTI:{fpath}')
                continue

            if fpath.lower().endswith('.bval'):
                bval_path = utils_dcm2nii.sanitize_filename(fpath)
            elif fpath.lower().endswith('.bvec'):
                bvec_path = utils_dcm2nii.sanitize_filename(fpath)
            elif fpath.lower().endswith('.nii.gz'):
                nifti_list.append(utils_dcm2nii.sanitize_filename(fpath))
            elif fpath.lower().endswith('.json'):
                json_path = utils_dcm2nii.sanitize_filename(fpath)
            else:
                pass

        # more than one NIFTI
        if len(nifti_list) > 1:
            logging.info('dcm2nii:multiple NIFTI')

        # Upload the dicom zip
        utils_xnat.upload_dirzip(dicomdir, scan_object.resource('DICOMZIP'))

        # Upload the NIFTIs
        utils_xnat.upload_files(nifti_list, scan_object.resource('NIFTI'))

        if os.path.isfile(bval_path) and os.path.isfile(bvec_path):
            logging.info('uploading BVAL/BVEC')
            utils_xnat.upload_file(bval_path, scan_object.resource('BVAL'))
            utils_xnat.upload_file(bvec_path, scan_object.resource('BVEC'))

        if os.path.isfile(json_path):
            logging.info(f'uploading JSON:{json_path}')
            utils_xnat.upload_file(json_path, scan_object.resource('JSON'))

    def upload_session(self, session_dir, project, subject, session):
        # session dir - should only contain a subfolder for each series with
        # as created by rename_dicom()

        session_exists = False

        # Check that project exists
        if not self._xnat.select_project(project).exists():
            logging.info('project does not exist, refusing to create')
            return

        # Check that subject exists, create as needed
        subject_object = self._xnat.select_subject(project, subject)
        if not subject_object.exists():
            logging.info(f'subject does not exist, creating:{subject}')
            subject_object.create()
        else:
            logging.info(f'subject exists:{subject}')

        session_object = subject_object.experiment(session)
        if not session_object.exists():
            logging.info(f'session does not exist, will be created later')
            # wait until get have attributes from json file: date, modality
        else:
            logging.info(f'session exists:{session}')
            session_exists = True

        # Handle each scan
        for p in sorted(pathlib.Path(session_dir).iterdir()):
            scan = p.name
            scan_object = session_object.scan(scan)

            if session_exists and scan_object.exists():
                logging.info(f'scan exists, skipping:{scan}')
                continue

            logging.info(f'uploading scan:{scan}')
            self._upload_scan(p, scan_object)
            logging.info(f'finished uploading scan:{scan}')

    def import_dicom_xnat(self, src, proj, subj, sess):

        with tempfile.TemporaryDirectory() as temp_dir:

            # Download all inputs
            if src.count('/') == 3:
                # Download specified scan
                s_proj, s_subj, s_sess, s_scan = src.split('/')
                logging.info(f'download DICOM:{s_proj}:{s_sess}:{s_scan}')
                scan = self._xnat.select_scan(s_proj, s_subj, s_sess, s_scan)
                scan.resource('DICOM').get(temp_dir, extract=True)
            else:
                # Download all session scans DICOM
                s_proj, s_subj, s_sess = src.split('/')

                # connect to the src session
                sess_object = self._xnat.select_session(s_proj, s_subj, s_sess)

                # download each dicom zip
                for scan in sess_object.scans():
                    s_scan = scan.label()
                    if not scan.resource('DICOM').exists():
                        continue

                    logging.info(f'download DICOM:{s_proj}:{s_sess}:{s_scan}')
                    scan.resource('DICOM').get(temp_dir, extract=True)

            # Upload them
            logging.info(f'uploading session:{temp_dir}:{proj}:{subj}:{sess}')
            import_dicom_dir(self, temp_dir, proj, subj, sess)

    # Pass tasks from garjus to dax by writing files to DISKQ
    def queue2dax(self):
        from .tasks import garjus2dax
        garjus2dax.queue2dax(self)

    # Update queue from dax
    def dax2queue(self):
        from .tasks import dax2garjus
        dax2garjus.dax2queue(self)


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    g = Garjus()
    print(g.projects())
    print(g.scans())
    print(g.assessors())
