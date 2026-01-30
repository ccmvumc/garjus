"""main garjus class.

Interactions with XNAT and Garjus REDCap should be via the main Garjus class.
Anything outside this class should refer to scans, assessors, issues, etc.

"""
import pathlib
import logging
import json
from datetime import datetime, date
import glob
import os
import tempfile
import shutil
import yaml

import pandas as pd
from redcap import Project, RedcapError
from pyxnat import Interface
from requests.exceptions import ConnectionError
from flask_login import current_user
from dax.XnatUtils import get_interface

from .subjects import load_subjects
from .subjects.covariates import export as covariates_export
from . import utils_redcap
from . import utils_xnat
from . import utils_dcm2nii
from .progress import update as update_progress
from .progress import make_project_report, make_stats_csv, make_export_zip, make_statshot, make_anonshot
from .compare import make_double_report, update as update_compare
from .stats import update as update_stats
from .automations import update as update_automations
from .image03 import update as update_image03, download as download_image03
from .issues import update as update_issues
from .import_dicom import import_dicom_zip, import_dicom_url, import_dicom_dir
from .dictionary import COLUMNS, PROCLIB, STATLIB
from .dictionary import ACTIVITY_RENAME, PROCESSING_RENAME, ISSUES_RENAME, REPORTS_RENAME
from .dictionary import TASKS_RENAME, ANALYSES_RENAME
from .tasks import update as update_tasks
from .analyses import run_analysis, download_resources, download_scan_resources
from .scans import update as update_scans
from .spin import run_spin
from .petmatch import match_pets
from .anonymize import anonymize_project, check_project


logger = logging.getLogger('garjus')


DISABLED_STATS = ['dti_PSMD_v1', 'fmri_rest_v4', 'fmri_rest_v5', 'struct_preproc_noflair_v1', 'francois_schaefer200_v1', 'francois_schaefer400_v1']


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
        xnat_interface: Interface=None,
        rcq_project: Project=None
    ):
        """Initialize garjus."""
        self._disconnect_xnat = False
        self._xnat = None
        self._rc = None
        self._rcq = None
        self._user = 'UnknownUser'
        self._rc = (redcap_project or self._default_redcap())
        self._rcq = (rcq_project or self._default_rcq())

        try:
            if current_user.is_authenticated:
                hostname = current_user.hostname
                self._user = current_user.id
                logger.debug(f'current user is:{self._user}')

                self._xnat = self._alias_xnat(hostname, self._user)
            else:
                raise Exception('user not authenticated')
        except Exception as err:
            logger.debug(err)

            if xnat_interface:
                self._xnat = xnat_interface
            else:
                try:
                    self._xnat = self._default_xnat()
                    self._disconnect_xnat = True
                except Exception as err2:
                    logger.debug(f'could not connect to XNAT:{err2}')


        if self._user == 'admin':
            # Prevent xnat admin user from accessing redcap
            logger.debug('refusing to connect xnat admin to garjus redcap')
            self._rc = None

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
        self.reports_rename = REPORTS_RENAME
        self.xsi2mod = utils_xnat.XSI2MOD
        self.max_stats = 350
        self._projects = None
        self._project2stats = {}
        self._columns = self._default_column_names()
        self._yamldir = self.set_yamldir()
        self._tempdir = tempfile.mkdtemp()
        self._our_assessors = set()
        self._cachedir = os.path.expanduser('~/.garjus')

        try:
            os.makedirs(self._cachedir)
        except FileExistsError:
            pass

        logger.debug(f'user:{self._user}')

        if not self._rc:
            logger.debug('main REDCap disabled')
        else:
            logger.debug(f'main REDCap:{self._rc.export_project_info()["project_id"]}')

        if not self._rcq:
            logger.debug('rcq REDCap disabled')
        else:
            logger.debug(f'rcq REDCap:{self._rcq.export_project_info()["project_id"]}')

    def __del__(self):
        """Close connectinons we opened."""
        if self._disconnect_xnat:
            try:
                logger.debug('disconnecting xnat')
                self._xnat.disconnect()
            except Exception:
                pass

    @staticmethod
    def userdir():
        username = 'UnknownUser'

        try:
            if current_user.is_authenticated:
                username = current_user.id
        except Exception as err:
            logger.debug(err)

        userdir = f'{os.path.expanduser("~/.garjus")}/{username}'

        try:
            os.makedirs(userdir)
        except FileExistsError:
            pass

        return userdir

    @staticmethod
    def login(hostname, username, password):

        xnat = get_interface(host=hostname, user=username, pwd=password)

        uri = '/data/services/tokens/issue'
        result = json.loads(xnat._exec(uri, 'GET'), strict=False)

        alias = result.get('alias')
        token = result.get('secret')

        # Save the token
        with open(f'{username}.token', 'w') as f:
            f.write(f'{alias},{token}')

        return (alias, token)

    @staticmethod
    def _default_xnat():
        return get_interface()

    @staticmethod
    def _alias_xnat(hostname, username):
        alias = None
        token = None

        try:
            with open(f'{username}.token') as f:
                (alias, token) = f.readline().strip().split(',')
        except Exception:
            raise Exception('failed to load alias credentials:{username}')

        return get_interface(user=alias, pwd=token)

    @staticmethod
    def _default_redcap():
        rc = None

        try:
            rc = utils_redcap.get_main_redcap()
        except FileNotFoundError as err:
            logger.debug(err)

        return rc

    @staticmethod
    def _default_rcq():
        rcq = None

        try:
            rcq = utils_redcap.get_rcq_redcap()
        except FileNotFoundError as err:
            logger.debug(err)

        return rcq

    @staticmethod
    def redcap_found():
        try:
            _main = utils_redcap.get_main_redcap()
            if _main:
                return True
            else:
                return False
        except Exception as err:
            logger.debug(err)
            return False

    @staticmethod
    def rcq_found():
        try:
            rcq = utils_redcap.get_rcq_redcap()
            if rcq:
                return True
            else:
                return False
        except Exception as err:
            logger.debug(err)
            return False

    @staticmethod
    def xnat_found():
        try:
            get_interface()
            return True
        except Exception:
            return False

    @staticmethod
    def is_authenticated():
        username = current_user.id
        try:
            with open(f'{username}.token') as f:
                (alias, token) = f.readline().strip().split(',')
                get_interface(user=alias, pwd=token)

            return True

        except Exception as err:
            logger.debug(f'could not load credentials:{username}:{err}')
            return False

    def cachedir(self):
        return self._cachedir

    def rcq_enabled(self):
        return (self._rcq is not None)

    def redcap_enabled(self):
        return (self._rc is not None)

    def xnat_enabled(self):
        return (self._xnat is not None)

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
        def_field = self._rc.def_field


        if not self.redcap_enabled():
            logger.info('cannot load activity, redcap not enabled')
            return None

        if project:
            projects = [project]
        else:
            projects = self.projects()

        rec = self._rc.export_records(
            records=projects,
            forms=['activity'],
            fields=[def_field])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'activity']
        for r in rec:
            d = {
                'PROJECT': r[def_field],
                'STATUS': 'COMPLETE',
                'SOURCE': 'ccmutils'}
            for k, v in self.activity_rename.items():
                d[v] = r.get(k, '')

            data.append(d)

        df = pd.DataFrame(data, columns=self.column_names('activity'))

        if startdate:
            df = df[df.DATETIME >= startdate]

        return df

    def add_analysis(self, project, analysis_dir, analysis_name=None):
        """Add an analysis record."""
        analysis_id = None
        analysis_output = None
        analysis_datetime  = datetime.now().strftime("%Y-%m-%d-%H%M%S")

        if analysis_name:
            analysis_processor = analysis_name
            analysis_descrip = analysis_name
        else:
            analysis_processor = 'statshot'
            analysis_name = 'statshot'
            analysis_descrip = 'statshot'

        record = {
            self._rcq.def_field: project,
            'analysis_name': analysis_name,
            'analysis_processor': analysis_processor,
            'redcap_repeat_instrument': 'analyses',
            'redcap_repeat_instance': 'new',
            'analysis_output': analysis_datetime,
            'analysis_status': 'RUNNING',
            'analyses_complete': '1',
        }

        # Add new record
        try:
            logger.debug(f'importing record:{record}')
            _response = self._rcq.import_records([record])
            assert 'count' in _response
            logger.debug('analysis record created')
        except (ValueError, RedcapError, AssertionError) as err:
            logger.error(f'error uploading:{err}')
            return None

        # Find the new record to get the analysis id
        logger.debug('locating new record')

        try:
            _ids = utils_redcap.match_repeat(
                self._rcq,
                project,
                'analyses',
                'analysis_output',
                analysis_datetime)

            analysis_id = _ids[-1]

        except Exception as err:
            logger.error('failed to match new record')
            return None

        _id = str(analysis_id).zfill(3)
        analysis_output = f'{project}_A{_id}_{analysis_datetime}'

        logger.info(f'uploading new analysis:{analysis_output}')

        # Create new resource folder on xnat
        try:
            xnat_project = self.xnat().select_project(project)
            logger.debug(xnat_project.label())
            xnat_resource = xnat_project.resource(analysis_output)
            logger.debug(xnat_resource.label())
            xnat_resource.create()
        except Exception as err:
            logger.error('failed to create resource on xnat')
            return None

        # Upload files to xnat
        files = [f for f in os.listdir(analysis_dir) if not f.startswith('.')]
        for f in files:
            cur = f'{analysis_dir}/{f}'
            if os.path.isdir(cur):
                utils_xnat.upload_dirzip(cur, xnat_resource)
            else:
                utils_xnat.upload_file(cur, xnat_resource)

        # Finalize analysis on redcap
        record = {
            self._rcq.def_field: project,
            'redcap_repeat_instrument': 'analyses',
            'redcap_repeat_instance': analysis_id,
            'analysis_output': analysis_output,
            'analysis_status': 'READY',
            'analyses_complete': '2',
            'analysis_descrip': analysis_descrip,
        }
        try:
            _response = self._rcq.import_records([record])
            assert 'count' in _response
            logger.debug('analysis record finalized')
        except (ValueError, RedcapError, AssertionError) as err:
            logger.error(f'error uploading:{err}')
            return None

        return analysis_id

    def add_activity(
        self,
        project=None,
        category=None,
        description=None,
        subject=None,
        event=None,
        repeat=None,
        session=None,
        scan=None,
        field=None,
        actdatetime=None,
        result=None,
    ):
        """Add an activity record."""
        def_field = self._rc.def_field

        if not actdatetime:
            actdatetime = datetime.now()

        # Format for REDCap
        activity_datetime = actdatetime.strftime("%Y-%m-%d %H:%M:%S")

        record = {
            def_field: project,
            'activity_description': f'{description}:{result}',
            'activity_datetime': activity_datetime,
            'activity_event': event,
            'activity_repeat': repeat,
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
            logger.debug('activity record created')
        except (ValueError, RedcapError, AssertionError) as err:
            logger.error(f'error uploading:{err}')

    def assessors(self, projects=None, proctypes=None, sesstypes=None):
        """Query XNAT for all assessors of and return list of dicts."""
        if not projects:
            projects = self.projects()

        logger.debug(f'loading projects={projects}, proctypes={proctypes}')

        data = self._load_assr_data(projects, proctypes)

        # Build a dataframe
        df = pd.DataFrame(data, columns=self.column_names('assessors'))

        if sesstypes:
            df = df[df.SESSTYPE.isin(sesstypes)]

        df['DATE'] = pd.to_datetime(df['DATE'])

        return df

    def favorites(self):
        return utils_xnat.get_my_favorites(self.xnat())

    def used_scantypes(self, assessors, scans):
        """List of scantypes that are used as inputs to assessors."""
        scantypes = []
        scanset = set()

        # Build set of scans used as inputs
        for i, a in assessors.iterrows():
            _values = list(a['INPUTS'].values())
            scanset.update(_values)

        # Convert to list
        scanlist = list(scanset)

        # Extract the scans only, no assessors
        scanlist = [x for x in scanlist if '/scans/' in x]

        # Make it a dataframe
        df = pd.DataFrame({'SCAN': scanlist})

        # Merge with scans data
        df = pd.merge(
            df,
            pd.DataFrame(scans),
            how='left',
            left_on='SCAN',
            right_on='full_path')

        # Get list of unique scan types
        scantypes = df.SCANTYPE.unique()

        return scantypes

    def assessor_resources(self, project, proctype):
        """Query XNAT and return dict"""

        data = self._load_ares_data(project, proctype)

        return data

    def subject_assessor_resources(self, project):
        """Query XNAT and return dict"""

        data = self._load_sgp_res_data(project)

        return data

    def delete_proctype(self, project, proctype, procstatus=None, qcstatus=None):
        msg = f'deleting assessors:{project=}:{proctype=}:{procstatus=}:{qcstatus=}'
        logger.info(msg)

        # Get list of assessors of proctype from project
        assessors = self.assessors(projects=[project], proctypes=[proctype])

        if procstatus:
            logger.debug(f'filter proctatus:{procstatus}')
            assessors = assessors[assessors.PROCSTATUS == procstatus]

        if qcstatus:
            logger.debug(f'filter qcstatus:{qcstatus}')
            assessors = assessors[assessors.QCSTATUS == qcstatus]

        # Delete them
        for a in sorted(assessors.ASSR.unique()):
            logger.info(f'deleting assessor:{a}')
            self.delete_assessor(project, a)

        # Also SGP
        assessors = self.subject_assessors(
            projects=[project], proctypes=[proctype])

        if assessors.empty:
            return

        if procstatus:
            logger.debug(f'filter proctatus:{procstatus}')
            assessors = assessors[assessors.PROCSTATUS == procstatus]

        if qcstatus:
            logger.debug(f'filter qcstatus:{qcstatus}')
            assessors = assessors[assessors.QCSTATUS == qcstatus]

        for a in sorted(assessors.ASSR.unique()):
            logger.info(f'deleting assessor:{a}')
            self.delete_assessor(project, a)

    def delete_assessor(self, project, assessor):

        # Connect to the assessor on xnat
        if is_sgp_assessor(assessor):
            _subj = assessor.split('-x-')[1]
            assr = self.xnat().select(
                f'/projects/{project}/subjects/{_subj}/experiment/{assessor}')
        else:
            assr = self.xnat().select_assessor(
                project,
                assessor.split('-x-')[1],
                assessor.split('-x-')[2],
                assessor)

        # Delete from xnat
        if assr.exists():
            logger.debug(f'deleting assessor from xnat:{assessor}')
            assr.delete()

        # Delete from task queue
        task_id = self.assessor_task_id(project, assessor)
        if task_id:
            logger.debug(f'deleting assessor from redcap taskqueue:{task_id}')
            payload = {
                'action': 'delete',
                'returnFormat': 'json',
                'content': 'record',
                'format': 'json',
                'instrument': 'taskqueue',
                'token': self._rcq.token,
                'records[0]': project,
                'repeat_instance': task_id}
            self._rcq._call_api(payload, 'del_record')

    def subject_assessors(self, projects=None, proctypes=None):
        """Query XNAT for all subject assessors, return dataframe."""
        if not projects:
            projects = self.projects()

        data = self._load_sgp_data(projects, proctypes)

        # Build a dataframe
        df = pd.DataFrame(data, columns=self.column_names('sgp'))

        df['DATE'] = pd.to_datetime(df['DATE'])

        return df

    def column_names(self, datatype):
        """Return list of colum names for this data type."""
        return self._columns.get(datatype)

    def issues(self, project=None):
        """Return the current existing issues data as list of dicts."""
        data = []
        def_field = self._rc.def_field

        if project:
            projects = [project]
        else:
            # only our projects
            projects = self.projects()

        if not self.redcap_enabled():
            logger.info('cannot load issues, redcap not enabled')
            return None

        # Get the data from redcap
        _fields = [def_field]

        # Only the specified project
        rec = self._rc.export_records(
            records=projects,
            forms=['issues'],
            fields=_fields,
        )

        # Only unresolved issues
        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'issues']
        rec = [x for x in rec if str(x['issues_complete']) != '2']

        # Reformat each record
        for r in rec:
            d = {'PROJECT': r[def_field], 'STATUS': 'FAIL'}
            for k, v in self.issues_rename.items():
                d[v] = r.get(k, '')

            data.append(d)

        # Finally, build a dataframe
        return pd.DataFrame(data, columns=self.column_names('issues'))

    def tasks(self, download=False, hidedone=True, projects=None):
        """List of task records."""
        DONE_LIST = ['COMPLETE', 'JOB_FAILED', 'DELETED']
        data = []
        def_field = self._rcq.def_field

        if projects:
            projects = [x for x in projects if x in self.projects()]
        else:
            projects = self.projects()

        # Load task records
        rec = self._rcq.export_records(
            records=projects,
            forms=['taskqueue'],
            fields=[def_field])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'taskqueue']

        if hidedone:
            rec = [x for x in rec if x['task_status'] not in DONE_LIST]

        df = pd.DataFrame(rec)
        if df.empty:
            return pd.DataFrame(columns=self.column_names('tasks'))

        # Load instance names 
        rec2 = self._rcq.export_records(
            records=projects,
            fields=[def_field, 'gen_daxinstance'],
            raw_or_label='label')

        project2user = {x[def_field]: x['gen_daxinstance'] for x in rec2 if x['gen_daxinstance']}

        df['PROJECT'] = df[def_field]
        df['USER'] = df['PROJECT'].map(project2user)
        df['ID'] = df['redcap_repeat_instance'].astype(str)

        # Make ID link back to redcap
        _url = self.redcap_url()
        _version = self.redcap_version()
        _pid = self.rcq_pid()
        if _url.endswith('/api/'):
            _url = _url[:-5]

        df['IDLINK'] = _url + '/redcap_v' + _version + '/DataEntry/index.php?pid=' + _pid + '&page=taskqueue&id=' + df['PROJECT'] + '&instance=' + df['ID']

        df['PROCTYPE'] = ''
        df['IMAGEDIR'] = ''
        df['JOBTEMPLATE'] = ''
        df = df.rename(columns=self.tasks_rename)
        df = df[self.column_names('tasks')]

        return df

    def save_task_yaml(self, project, task_id, yaml_dir):
        return utils_redcap.download_named_file(
            self._rcq,
            project,
            'task_yamlupload',
            yaml_dir,
            repeat_id=task_id)

    def set_task_statuses(self, tasks):
        records = []
        def_field = self._rcq.def_field

        # Build list of task updates as records with updated values
        for i, t in tasks.iterrows():
            r = {
                def_field: t['PROJECT'],
                'redcap_repeat_instance': t['ID'],
                'redcap_repeat_instrument': 'taskqueue',
                'task_status': t['STATUS']
            }
            if t['STATUS'] == 'COMPLETE':
                # Set the redcap complete indicator too
                r['taskqueue_complete'] = '2'

            # TODO: get the job id here too

            records.append(r)

        # Apply the updates in one call
        try:
            response = self._rcq.import_records(records)
            assert 'count' in response
            logger.debug('task status records updated')
        except AssertionError as err:
            logger.error(f'failed to set task statuses:{err}')

    def set_task_status(self, project, task_id, status):
        def_field = self._rcq.def_field

        record = {
            def_field: project,
            'redcap_repeat_instance': task_id,
            'redcap_repeat_instrument': 'taskqueue',
            'task_status': status,
        }

        if status == 'COMPLETE':
            record['taskqueue_complete'] = '2'
        elif status == 'JOB_FAILED':
            record['taskqueue_complete'] = '0'

        try:
            response = self._rcq.import_records([record])
            assert 'count' in response
            logger.debug('task status updated')
        except AssertionError as err:
            logger.error(f'failed to set task status:{err}')

    def delete_old_issues(self, projects=None, days=7):
        old_issues = []
        def_field = self._rc.def_field

        if not self.redcap_enabled():
            logger.info('cannot delete issues, redcap not enabled')
            return None

        if projects:
            projects = [x for x in projects if x in self.projects()]
        else:
            projects = self.projects()

        # Get the data from redcap
        _fields = [def_field]
        # Only the specified project
        rec = self._rc.export_records(
            records=projects,
            forms=['issues'],
            fields=_fields,
        )

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
                _main = r[def_field]
                _id = r['redcap_repeat_instance']
                logger.debug(f'{_main}:{_id}:{days_old} days old')
                old_issues.append(r)

        # Apply delete to list of old issues
        self.delete_issues(old_issues)

    def delete_bad_tasks(self):
        def_field = self._rcq.def_field
        instrument = 'taskqueue'

        if not self.redcap_enabled():
            logger.info('cannot delete bad tasks, redcap not enabled')
            return None

        rec = self._rcq.export_records(forms=[instrument], fields=[def_field])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == instrument]

        # Tasks with no name set to running are created when a deleted task gets updated
        rec = [x for x in rec if x['task_assessor'] == '' and x['task_status'] == 'RUNNING']

        logger.info(f'deleting {len(rec)} bad tasks')
        self.delete_tasks(rec)

    def import_dicom(self, src, dst):
        """Import dicom source to destination."""
        logger.debug(f'uploading from:{src}')

        if dst.count('/') == 3:
            (proj, subj, sess, scan) = dst.split('/')
            logger.debug(f'uploading to:{proj},{subj},{sess},{scan}')
            return self.upload_scan(src, proj, subj, sess, scan)

        (proj, subj, sess) = dst.split('/')
        logger.debug(f'uploading to:{proj},{subj},{sess}')

        if src.endswith('.zip'):
            import_dicom_zip(self, src, proj, subj, sess)
        elif src.startswith('http'):
            # e.g. gstudy link
            import_dicom_url(self, src, proj, subj, sess)
        elif os.path.isdir(src):
            import_dicom_dir(self, src, proj, subj, sess)
        else:
            self.import_dicom_xnat(src, proj, subj, sess)

        logger.debug(f'adding activity:{src}')
        self.add_activity(
            project=proj,
            category='import_dicom',
            description=src,
            subject=subj,
            session=sess,
            result='COMPLETE')

    def import_nifti(self, src, dst, modality='MR'):
        """Import nifti source to destination."""
        logger.debug(f'uploading from:{src}')

        if dst.count('/') == 4:
            (proj, subj, sess, scan, scantype) = dst.split('/')
            logger.debug(f'uploading to:{proj},{subj},{sess},{scan},{scantype}')
            self.upload_nifti(src, proj, subj, sess, scan, scantype, modality)
        else:
            logger.error(f'invalid dst:{dst}')
            return

        logger.debug(f'adding activity:{src}')
        self.add_activity(
            project=proj,
            category='import_nifti',
            description=src,
            subject=subj,
            session=sess,
            scan=scan,
            result='COMPLETE')

    def import_edat(self, src, dst):
        """Import edat source to destination."""
        logger.debug(f'uploading from:{src}')

        if dst.count('/') == 3:
            (proj, subj, sess, scan) = dst.split('/')
            logger.debug(f'uploading to:{proj},{subj},{sess},{scan}')
            self.upload_edat(src, proj, subj, sess, scan)
        else:
            logger.error(f'invalid dst:{dst}')
            return

        logger.debug(f'adding activity:{src}')
        self.add_activity(
            project=proj,
            category='import_edat',
            description=f'{src}:{dst}',
            subject=subj,
            session=sess,
            scan=scan,
            result='COMPLETE')

    def copy_sess(self, src, dst):
        """Copy dicom source to destination."""
        logger.debug(f'copy from:{src}')
        logger.debug(f'copy to:{dst}')
        (s_proj, s_subj, s_sess) = src.split('/')
        (d_proj, d_subj, d_sess) = dst.split('/')
        self._copy_session(s_proj, s_subj, s_sess, d_proj, d_subj, d_sess)
        self.add_activity(
            project=d_proj,
            category='copy_sess',
            description=src,
            subject=d_subj,
            session=d_sess,
            result='COMPLETE')

    def copy_scan(self, src, dst):
        """Copy scan source to destination."""
        logger.debug(f'copy from:{src}')
        logger.debug(f'copy to:{dst}')
        (s_proj, s_subj, s_sess, s_scan) = src.split('/')
        (d_proj, d_subj, d_sess, d_scan) = dst.split('/')

        self._copy_scan(
            s_proj,
            s_subj,
            s_sess,
            s_scan,
            d_proj,
            d_subj,
            d_sess,
            d_scan)

        self.add_activity(
            project=d_proj,
            category='copy_scan',
            description=src,
            subject=d_subj,
            session=d_sess,
            scan=d_scan,
            result='COMPLETE')

    def set_session_type(self, src, sesstype):
        """Set Session Type in XNAT."""
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        (s_proj, s_subj, s_sess) = src.split('/')

        logger.debug(f'{s_proj}:{s_sess}:setting session type:{sesstype}')

        self._xnat.select_session(s_proj, s_subj, s_sess).attrs.set(
            'session_type', sesstype)

        self.add_activity(
            project=s_proj,
            category='set_session_type',
            description=src,
            subject=s_subj,
            session=s_sess,
            result='COMPLETE')

    def set_session_site(self, src, site):
        """Set site in XNAT."""
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        (s_proj, s_subj, s_sess) = src.split('/')

        logger.debug(f'{s_proj}:{s_sess}:setting site:{site}')

        self._xnat.select_session(s_proj, s_subj, s_sess).attrs.set(
            'xnat:imagesessiondata/acquisition_site', site)

        self.add_activity(
            project=s_proj,
            category='set_session_site',
            description=src,
            subject=s_subj,
            session=s_sess,
            result='COMPLETE')

    def scans(
        self,
        projects=None,
        scantypes=None,
        modalities=None,
        sites=None,
        startdate=None,
        enddate=None,
        sesstypes=None,
    ):
        """Query XNAT for all scans and return a dictionary of scan info."""
        if not projects:
            projects = self.projects()

        data = self._load_scan_data(projects, scantypes, modalities, sites)

        df = pd.DataFrame(data, columns=self.column_names('scans'))

        if sesstypes:
            df = df[df.SESSTYPE.isin(sesstypes)]

        # Format as datetime
        df['DATE'] = pd.to_datetime(df['DATE'])

        if startdate:
            # Filter to begin with startdate
            df = df[df.DATE >= startdate]

        if enddate:
            # Filter end
            df = df[df.DATE <= enddate]

        if self.redcap_enabled():
            # Merge in auxiliary scan data
            dfp = pd.DataFrame()
            for p in projects:
                logger.debug(f'loading scan stats:{p}')
                dfp = pd.concat([dfp, self._load_scan_stats(p)])

            index_cols = ['SUBJECT', 'SESSION', 'SCANID']

            drop_cols = [x for x in dfp.columns if x in df.columns and x not in index_cols]

            df = df.drop(columns=drop_cols)

            df = pd.merge(
                df,
                dfp,
                how='left',
                on=index_cols,
            )

        # Return as dataframe
        df = df.sort_values('full_path')
        return df

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
        if not self.redcap_enabled():
            logger.info('cannot load sites, redcap not enabled')
            return None

        if project:
            projects = [project]
        else:
            projects = self.projects()

        return self._rc.export_records(records=projects, forms=['sites'])

    def _load_project_names(self):

        # Get list of projects in redcap
        if self.redcap_enabled():
            _records = self._rc.export_records(fields=[self._rc.def_field, 'main_complete'])
            redcap_names = [x[self._rc.def_field] for x in _records]
            logger.debug(f'redcap projects={redcap_names}')
        else:
            logger.debug('redcap not available')
            redcap_names = None

        # Get list of projects in xnat
        if self.xnat_enabled():
            if self._user == 'admin':
                logger.debug('loading admin projects')
                xnat_names = utils_xnat.get_admin_projects(self._xnat)
            else:
                logger.debug(f'not admin, loading user projects:{self._user}')
                # Load from xnat
                xnat_names = utils_xnat.get_my_projects(self.xnat())

            logger.debug(f'xnat projects={xnat_names}')
        else:
            logger.debug('xnat not available')
            xnat_names = None

        self._xnat_projects = xnat_names
        self._rc_projects = redcap_names

    def _default_column_names(self):
        return COLUMNS

    def _stats_redcap(self, project):
        def_field = self._rc.def_field
        stats_redcap = None

        if not self.redcap_enabled():
            logger.info('cannot load stats, redcap not enabled')
            return None

        if project not in self._project2stats:
            # get the project ID for the stats redcap for this project
            _fields = [def_field, 'project_stats']
            rec = self._rc.export_records(records=[project], fields=_fields)
            rec = [x for x in rec if x[def_field] == project][0]
            redcap_id = rec['project_stats']

            if len(redcap_id) == 32:
                logger.debug(f'loading redcap with key:{project}')
                stats_redcap = utils_redcap.get_redcap(api_key=redcap_id)
            else:
                logger.debug(f'loading redcap with id:{project}')
                stats_redcap = utils_redcap.get_redcap(project_id=redcap_id)

            # Save it
            self._project2stats[project] = stats_redcap
        else:
            stats_redcap = self._project2stats[project]

        return stats_redcap

    def switch_status(
        self,
        project,
        proctype,
        oldstatus,
        newstatus,
        sesstype,
        session
    ):
        # Switch assessor status in xnat/redcap, e.g. from JOB_FAILED to COMPLETE
        # Load assessors
        #df = self.assessors(
        #    projects=[project],
        #    proctypes=[proctype],
        #    sesstypes=sesstype
        #)

        # get tasks
        df = self.tasks(hidedone=False)
        df = df[(df.PROJECT == project) & (df.YAMLUPLOAD.str.startswith(proctype))]

        # Filter
        df = df[df.STATUS == oldstatus]

        logger.info(f'set from {oldstatus} to {newstatus}:{len(df)}')

        # Set statuses
        for i, t in df.iterrows():
            assr = t['ASSESSOR']

            # Connect to the assessor on xnat
            if is_sgp_assessor(t['ASSESSOR']):
                xsitype = 'proc:subjgenprocdata'
                _subj = assr.split('-x-')[1]
                assessor = self.xnat().select(f'/projects/{project}/subjects/{_subj}/experiment/{assr}')
            else:
                xsitype = 'proc:genprocdata'
                assessor = self.xnat().select_assessor(
                    project,
                    assr.split('-x-')[1],
                    assr.split('-x-')[2],
                    assr)

            if not assessor.exists():
                logger.debug(f'assessor not found on xnat:{assr}')
                continue

            logger.info(f'setting xnat attributes:{project}:{assr}:{newstatus}')
            assessor.attrs.set(f'{xsitype}/procstatus', newstatus)

            task_id = t['ID']
            logger.info(f'setting REDCap attributes:{project}:{task_id}:{newstatus}')
            self.set_task_status(project, task_id, newstatus)

    def analyses(self, projects=None, download=True):
        """Return analyses."""
        data = []
        def_field = self._rcq.def_field

        if not self.rcq_enabled():
            logger.info('cannot load analyses, redcap not enabled')
            return None

        logger.debug(f'analyses projects={projects}')

        if projects:
            projects = [x for x in projects if x in self.projects()]
        else:
            projects = self.projects()

        rec = self._rcq.export_records(
            records=projects,
            forms=['analyses'],
            fields=[def_field])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'analyses']

        # Apply hideshow
        rec = [x for x in rec if x['analysis_hideshow'] != '1']

        for r in rec:
            # Initialize record
            project_id = r[def_field]
            repeat_id = r['redcap_repeat_instance']
            link = self.get_link('analyses', project_id, repeat_id)
            d = {
                'PROJECT': project_id,
                'ID': repeat_id,
                'EDIT': link,
            }

            # Get renamed variables
            for k, v in self.analyses_rename.items():
                d[v] = r.get(k, '')

            if r['analysis_procrepo']:
                d['PROCESSOR'] = r['analysis_procrepo']

            # Download the yaml file and load it too
            if download and d['PROCESSOR']:
                logger.debug(f'loading:{d["PROCESSOR"]}')
                with tempfile.TemporaryDirectory() as temp_dir:
                    yaml_file = utils_redcap.download_named_file(
                        self._rcq,
                        d['PROJECT'],
                        'analysis_processor',
                        temp_dir,
                        repeat_id=d['ID'],
                    )

                    # Load yaml contents
                    try:
                        with open(yaml_file, "r") as f:
                            d['PROCESSOR'] = yaml.load(f, Loader=yaml.FullLoader)
                    except yaml.error.YAMLError as err:
                        logger.error(f'failed to load yaml:{yaml_file}:{err}')

            # Finally, add to our list
            data.append(d)

        return pd.DataFrame(data, columns=self.column_names('analyses'))

    def load_analysis(self, project, analysis_id, download=True):
        """Return analysis protocol record."""
        def_field = self._rcq.def_field

        if not self.redcap_enabled():
            logger.info('cannot load analysis, redcap not enabled')
            return None

        data = {
            'PROJECT': project,
            'ID': analysis_id,
        }

        rec = self._rcq.export_records(
            fields=[def_field],
            forms=['analyses'],
            records=[project],
        )

        rec = [x for x in rec if str(x['redcap_repeat_instance']) == analysis_id]

        # Get renamed variables
        for k, v in self.analyses_rename.items():
            data[v] = rec[0].get(k, '')

        # Download the yaml file and load it too
        if data['PROCESSOR']:
            logger.debug(f'loading:{data["PROCESSOR"]}')
            with tempfile.TemporaryDirectory() as temp_dir:
                yaml_file = utils_redcap.download_named_file(
                    self._rcq,
                    project,
                    'analysis_processor',
                    temp_dir,
                    repeat_id=analysis_id)

                # Load yaml contents
                try:
                    with open(yaml_file, "r") as f:
                        data['PROCESSOR'] = yaml.load(f, Loader=yaml.FullLoader)
                except yaml.error.YAMLError as err:
                    logger.error(f'failed to load yaml file{yaml_file}:{err}')
                    return None

        return data

    def acols(self):
        return [
            'ASSR',
            'PROJECT',
            'SUBJECT',
            'SESSION',
            'SESSTYPE',
            'SITE',
            'DATE',
            'PROCTYPE',
        ]

    def stats(
        self,
        project,
        assessors=None,
        proctypes=None,
        sesstypes=None,
        persubject=False
    ):
        """Return all stats for project, filtered by proctypes."""

        if not self.redcap_enabled():
            logger.info('cannot load stats, redcap not enabled')
            return None

        try:
            """Get the stats data from REDCap."""
            statsrc = self._stats_redcap(project)
            rec = statsrc.export_records(forms=['stats'])

        except:
            return pd.DataFrame(columns=['ASSR', 'PROCTYPE', 'SESSTYPE'])

        # Filter out FS6 if found, old
        rec = [x for x in rec if 'FS6_v1' not in x['stats_assr']]

        # Filter out old FS7 if found
        rec = [x for x in rec if not x['stats_name'].startswith('fs7_')]

        # Make a dataframe of columns we need
        df = pd.DataFrame(
            rec,
            columns=['stats_assr', 'stats_name', 'stats_value'])

        df = df.drop_duplicates(subset=['stats_assr', 'stats_name'])

        # Pivot to row per assessor, col per stats_name, values as stats_value
        df = pd.pivot(
            df,
            index='stats_assr',
            values='stats_value',
            columns='stats_name')

        df = df.reset_index()

        if assessors is None:
            assessors = self.assessors(projects=[project], proctypes=proctypes)
            assessors = pd.concat([assessors, self.subject_assessors(projects=[project], proctypes=proctypes)])

        # Filter out failed
        logger.debug(f'filtering out Failed assessors')
        assessors = assessors[~assessors.QCSTATUS.isin(['Failed'])]

        # Merge with assessors
        df = pd.merge(
            assessors[self.acols()], df, left_on='ASSR', right_on='stats_assr')

        # Clean up
        df = df.drop(columns=['stats_assr'])
        df = df.dropna(axis=1, how='all')

        if df.empty:
            return pd.DataFrame(columns=['ASSR', 'PROCTYPE', 'SESSTYPE'])

        df = df.sort_values('ASSR')

        # Apply filters
        if proctypes:
            df = df[df.PROCTYPE.isin(proctypes)]

        if sesstypes:
            df = df[df.SESSTYPE.isin(sesstypes)]

        if persubject:
            logger.debug(f'pivot to row per subject')

            # Pivot to row per subject
            df = _subject_pivot(df)

        if 'fmri_msit_v4' in list(df.PROCTYPE.unique()):
            df = _get_msit(df)

        return df


    def stats_assessors(self, project, proctypes=None):
        """Get list of assessors already in stats archive."""

        if not self.redcap_enabled():
            logger.info('cannot load stats, redcap not enabled')
            return None

        statsrc = self._stats_redcap(project)

        _records = statsrc.export_records(fields=['stats_assr'])
        return list(set([x['stats_assr'] for x in _records]))


    def stats_scans(self, project, scantypes=None):
        """Get list of scans already in stats archive."""

        if not self.redcap_enabled():
            logger.info('cannot load stats, redcap not enabled')
            return None

        records = self._stats_redcap(project).export_records(
            fields=['subject_id', 'scan_session', 'scan_id'])

        records = [x for x in records if x['redcap_repeat_instrument'] == 'scan']

        return records

    def _load_scan_stats(
        self,
        project,
    ):
        """Return all stats for project, filtered."""

        if not self.redcap_enabled():
            logger.info('cannot load scan stats, redcap not enabled')
            return None

        try:
            """Get the stats data from REDCap."""
            logger.debug(f'loading scan stats from REDCap:{project}')
            records = self._stats_redcap(project).export_records(forms=['scan'])
            records = [x for x in records if x['redcap_repeat_instrument'] == 'scan']
        except:
            records = []

        if len(records) == 0:
            df = pd.DataFrame(columns=[
                'SUBJECT',
                'SESSION',
                'SCANID',
                'DURATION',
                'TR',
                'SENSE',
                'MB',
                'THICK',
            ])
        else:
            logger.debug(f'prepping scan stats:{project}')

            # Make a dataframe
            df = pd.DataFrame(records)
            df = df.drop(columns=[
                'scan_complete',
                'redcap_repeat_instrument',
                'redcap_repeat_instance'])

            df = df.rename(columns={
                'subject_id': 'SUBJECT',
                'scan_id': 'SCANID',
                'scan_session': 'SESSION',
                'scan_duration': 'DURATION',
                'scan_tr': 'TR',
                'scan_prfip': 'SENSE',
                'scan_proop': 'MB',
                'scan_slicethickness': 'THICK',
            })

            df = df.drop_duplicates(subset=['SESSION', 'SCANID'])

        return df

    def projects(self, autofilter=True):
        """Get list of projects."""
        if self._projects is None:
            # Loads projects names from xnat and redcap
            self._load_project_names()

        # Get combination of lists
        if self._rc_projects and self._xnat_projects:
            if autofilter:
                # Only redcap projects
                self._projects = self._rc_projects
            else:
                # Get the union
                self._projects = sorted(list(set(self._rc_projects) | set(self._xnat_projects)))
        elif self._rc_projects and not self._xnat_projects:
            self._projects = self._rc_projects
        elif self._xnat_projects:
            self._projects = self._xnat_projects
        else:
            self._projects = []

        logger.debug(f'my projects={self._projects}')

        return self._projects

    def subjects(self, project, include_dob=False):
        """Return subjects for project."""

        return load_subjects(self, project, include_dob)

    def petmatch(self, project):
        """Return PETs matched to MRIs for project."""

        return match_pets(self, project)

    def orphans(self, project):
        """Return orphaned assessors for project."""

        # Get list of existing assessors and scans
        dfs = self.scans(projects=[project])
        dfa = self.assessors(projects=[project])

        # Make a combined list of all existing artefacts
        existing = list(dfs.full_path.unique()) + list(dfa.full_path.unique())

        # Find any assessors with inputs not existing, child without parent
        dfa['ORPHAN'] = dfa.apply(lambda x: bool([a for a in x.INPUTS.values() if a not in existing]), axis=1)

        # Return list of assessor labels
        return sorted(list(dfa[dfa.ORPHAN == True].ASSR))

    def stattypes(self, project):
        """Get list of projects stat types."""
        types = []

        # Append others
        logger.debug(f'loading proctypes:{project}')
        protocols = self.processing_protocols(project)
        for i, row in protocols.iterrows():
            ptype = row['TYPE']
            if ptype not in types:
                logger.debug(f'appending proctype:{ptype}')
                types.append(ptype)

        types = [x for x in types if x not in DISABLED_STATS]

        return types

    def _get_proctype(self, procfile):
        # Get just the filename without the directory path
        tmp = os.path.basename(procfile)

        # Split on periods and grab the 4th value from right,
        # thus allowing periods in the main processor name
        return tmp.rsplit('.')[-4]

    def proctypes(self, project):
        """Get list of project proc types."""
        types = []

        # Append others
        protocols = self.processing_protocols(project)
        for i, row in protocols.iterrows():
            ptype = row['TYPE']
            if ptype not in types:
                logger.debug(f'appending proctype:{ptype}')
                types.append(ptype)

        return types

    def all_scantypes(self):
        """Get list of scan types."""
        types = []

        if not self.redcap_enabled():
            logger.info('cannot load scantypes, redcap not enabled')
            return None

        projects = self.projects()

        logger.debug(f'getting scantypes:{projects}')

        records = self._rc.export_records(
            records=projects,
            fields=['project_scanmap'])

        for r in records:
            scanmap = r['project_scanmap']
            scanmap = self._parse_scanmap(scanmap)
            proj_types = list(set([v for k, v in scanmap.items()]))
            logger.debug(f'scantypes:{r[self._rc.def_field]}:{proj_types}')
            types.extend(proj_types)

        # Make the list unique
        return list(set(types))

    def _parse_scanmap(self, scanmap):
        try:
            # Parse multiline string of delimited keyvalue pairs into dict
            scanmap = dict(
                x.strip().split(':', 1) for x in scanmap.split('\n'))

            # Remove extra whitespace from keys and values
            scanmap = {k.strip(): v.strip() for k, v in scanmap.items()}
        except (ValueError, AttributeError):
            scanmap = {}

        return scanmap

    def all_proctypes(self):
        """Get list of project proc types."""
        types = []

        if not self.redcap_enabled():
            logger.info('cannot load proctypes, redcap not enabled')
            return None

        # Get all processing records across projects
        try:
            rec = self._rc.export_records(forms=['processing'])
        except Exception as err:
            logger.error(err)
            return []

        rec = [x for x in rec if (('redcap_repeat_instrument' not in x) or (x['redcap_repeat_instrument'] == 'processing'))]
        rec = [x for x in rec if str(x['processing_complete']) == '2']

        for r in rec:
            if r['processor_yamlupload']:
                dtype = self._get_proctype(r['processor_yamlupload'])
            else:
                dtype = self._get_proctype(r['processor_file'])

            # Finally, add to our list
            types.append(dtype)

        return list(set(types))

    def scantypes(self, project):
        # Get the values from the key/value scan map and return unique list
        if not self.redcap_enabled():
            logger.info('cannot load scantypes, redcap not enabled')
            return None

        scanmap = self.scanmap(project)
        return list(set([v for k, v in scanmap.items()]))

    def scanmap(self, project):
        """Parse scan map stored as string into map."""
        try:
            scanmap = self.project_setting(project, 'scanmap')
        except Exception as err:
            logger.error(err)
            return {}

        try:
            # Parse multiline string of delimited keyvalue pairs into dict
            scanmap = dict(x.strip().split(':', 1) for x in scanmap.split('\n'))

            # Remove extra whitespace from keys and values
            scanmap = {k.strip(): v.strip() for k, v in scanmap.items()}
        except (ValueError, AttributeError):
            scanmap = {}

        return scanmap

    def _load_scan_data(
        self,
        projects=None,
        scantypes=None,
        modalities=None,
        sites=None
    ):
        """Get scan info from XNAT as list of dicts."""
        scans = []
        uri = self.scan_uri

        if projects is not None:
            uri += f'&project={",".join(projects)}'

        result = self._get_result(uri)

        # Get shared
        uri = self.scan_uri
        uri += f'&xnat:imagesessiondata/sharing/share/project={",".join(projects)}'
        result2 = self._get_result(uri)
        # Set project to shared name
        for r in result2:
            r['project'] = r['xnat:imagesessiondata/sharing/share/project']

        # Append shared
        result += result2

        # Change from one row per resource to one row per scan
        # TODO: use pandas pivot/melt?
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

        # Filter by modality
        if modalities:
            scans = [x for x in scans if x['MODALITY'] in modalities]

        # Filter by site
        if sites:
            scans = [x for x in scans if x['SITE'] in sites]

        return scans

    def _load_assr_data(self, projects=None, proctypes=None):
        """Get assessor info from XNAT as list of dicts."""
        assessors = []
        uri = self.assr_uri

        if projects is not None:
            uri += f'&project={",".join(projects)}'

        result = self._get_result(uri)

        # Get shared
        uri = self.assr_uri
        uri += f'&xnat:imagesessiondata/sharing/share/project={",".join(projects)}'
        result2 = self._get_result(uri)
        # Set project to shared name
        for r in result2:
            r['project'] = r['xnat:imagesessiondata/sharing/share/project']

        # Append shared
        result += result2

        for r in result:
            assessors.append(self._assessor_info(r))

        # Filter by type
        if proctypes is not None:
            assessors = [x for x in assessors if x['PROCTYPE'] in proctypes]

        return assessors

    def _load_ares_data(self, project, proctype):
        data = {}
        uri = self.assr_uri + ',proc:genprocdata/out/file/label'
        uri += f'&project={project}'

        result = self._get_result(uri)

        for r in result:
            assr = r.get('proc:genprocdata/label', '')
            res = r.get('proc:genprocdata/out/file/label', '')
            if not res:
                continue

            if assr in data.keys():
                # Append to list of resources
                data[assr] += ',' + res
            else:
                data[assr] = res

        return data


    def _load_sgp_res_data(self, project):
        data = {}
        uri = self.sgp_uri + ',proc:subjgenprocdata/resources/resource/label'
        uri += f'&project={project}'

        result = self._get_result(uri)

        for r in result:
            assr = r.get('proc:subjgenprocdata/label', '')
            res = r.get('proc:subjgenprocdata/resources/resource/label', '')
            if not res:
                continue

            if assr in data.keys():
                # Append to list of resources
                data[assr] += ',' + res
            else:
                data[assr] = res

        return data

    def _load_sgp_data(self, projects=None, proctypes=None):
        """Get assessor info from XNAT as list of dicts."""
        assessors = []
        uri = self.sgp_uri

        if projects:
            uri += f'&project={",".join(projects)}'

        logging.debug(f'get_result uri=:{uri}')
        result = self._get_result(uri)

        for r in result:
            assessors.append(self._sgp_info(r))

        # Filter by type
        if proctypes:
            assessors = [x for x in assessors if x['PROCTYPE'] in proctypes]

        return assessors

    def _get_result(self, uri):
        """Get result of xnat query."""
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        logger.debug(uri)
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

        info['XSITYPE'] = 'proc:subjgenprocdata'

        # Decode inputs into list
        info['INPUTS'] = utils_xnat.decode_inputs(info['INPUTS'])

        # Get the full path
        _p = '/projects/{0}/subjects/{1}/assessors/{2}'.format(
            info['PROJECT'],
            info['SUBJECT'],
            info['ASSR'])
        info['full_path'] = _p

        return info

    def reports(self, projects=None):
        data = []
        def_field = self._rc.def_field

        # only our projects
        if projects:
            projects = [x for x in projects if x in self.projects()]
        else:
            projects = self.projects()

        # Load Progress Reports
        for r in self.progress_reports(projects):
            d = {
                'PROJECT': r[def_field],
                'TYPE': 'Progress'}

            # Get renamed variables
            for k, v in self.reports_rename.items():
                if v not in d or d[v] == '':
                    d[v] = r.get(k, '')

            data.append(d)

        # Load Double Reports
        for r in self.double_reports(projects):
            d = {
                'PROJECT': r[def_field],
                'TYPE': 'Double'}

            # Get renamed variables
            for k, v in self.reports_rename.items():
                if v not in d or d[v] == '': 
                    d[v] = r.get(k, '')

            data.append(d)

        # Load NDAimage03 Reports
        for r in self.ndaimage03_reports(projects):
            d = {
                'PROJECT': r[def_field],
                'TYPE': 'NDAimage03'}

            # Get renamed variables
            for k, v in self.reports_rename.items():
                if v not in d or d[v] == '': 
                    d[v] = r.get(k, '')

            data.append(d)

        df = pd.DataFrame(data, columns=self.column_names('reports'))

        return df

    def progress_reports(self, projects=None):
        """List of progress records."""

        if not self.redcap_enabled():
            logger.info('cannot load progress reports, redcap not enabled')
            return None

        if projects:
            projects = [x for x in projects if x in self.projects()]
        else:
            projects = self.projects()

        rec = self._rc.export_records(
            records=projects,
            forms=['progress'],
            fields=[self._rc.def_field])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'progress']
        rec = [x for x in rec if str(x['progress_complete']) == '2']
        return rec

    def double_reports(self, projects=None):
        """List of double entry report records."""

        if not self.redcap_enabled():
            logger.info('cannot load double reports, redcap not enabled')
            return None

        if projects:
            projects = [x for x in projects if x in self.projects()]
        else:
            projects = self.projects()


        rec = self._rc.export_records(
            records=projects,
            forms=['double'],
            fields=[self._rc.def_field])


        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'double']
        rec = [x for x in rec if str(x['double_complete']) == '2']
        return rec

    def ndaimage03_reports(self, projects=None):
        """List of double entry report records."""

        if not self.redcap_enabled():
            logger.info('cannot load ndaimage03 reports, redcap not enabled')
            return None

        if projects:
            projects = [x for x in projects if x in self.projects()]
        else:
            projects = self.projects()

        rec = self._rc.export_records(
            records=projects,
            forms=['ndaimage03'],
            fields=[self._rc.def_field])


        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'ndaimage03']
        rec = [x for x in rec if str(x['ndaimage03_complete']) == '2']

        return rec

    def processing_protocols(self, project=None, download=False):
        """Return processing protocols."""
        data = []
        def_field = self._rcq.def_field

        if project:
            projects = [project]
        else:
            projects = self.projects()

        rec = self._rcq.export_records(
            records=projects,
            forms=['processing'],
            fields=[def_field])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'processing']

        # Only enabled processing
        rec = [x for x in rec if str(x['processing_complete']) == '2']

        for r in rec:
            # Initialize record with project
            project_id = r[def_field]
            repeat_id = r['redcap_repeat_instance']
            link = self.get_link('processing', project_id, repeat_id)
            d = {

                'PROJECT': project_id,
                'EDIT': link,
                'ID': repeat_id,

            }

            # Find the yaml file
            if r['processor_yamlupload']:
                filepath = r['processor_yamlupload']
                if download:
                    filename = os.path.join(
                        self._tempdir, r['processor_yamlupload'])
                    filepath = utils_redcap.download_file(
                        self._rcq,
                        project,
                        'processor_yamlupload',
                        filename,
                        repeat_id=r['redcap_repeat_instance'])
            else:
                filepath = r['processor_file']

            if not os.path.isabs(filepath):
                # Prepend lib location
                filepath = os.path.join(self._yamldir, filepath)

            if download and not os.path.isfile(filepath):
                logger.debug(f'file not found:{filepath}, download={download}')
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

    def update(self, projects=None, choices=None, types=None):
        """Update projects."""
        if not projects:
            projects = self.projects()

        if not choices:
            choices = [
                'automations',
                'stats',
                'issues',
                'progress',
                'compare',
                'scans',
            ]

        logger.debug(f'updating projects:{projects}:{choices}')

        if 'automations' in choices:
            logger.info('updating automations')
            update_automations(self, projects, autos_include=types)

        if 'issues' in choices:
            logger.info('updating issues')
            update_issues(self, projects)
            logger.debug('deleting old issues')
            self.delete_old_issues(projects)

        if 'stats' in choices:
            # Only run on intersect of specified projects and projects with
            # stats, such that if the list is empty, nothing will run
            logger.info('updating stats')
            _stats_projects = self.stats_projects()
            _projects = [x for x in projects if x in _stats_projects]
            update_stats(self, _projects, types)

        if 'progress' in choices:
            # confirm each project has report for current month with PDF & zip
            logger.info('updating progress')
            update_progress(self, projects)

        if 'compare' in choices:
            # confirm each project has report for current month
            logger.info('updating compare')
            try:
                update_compare(self, projects)
            except Exception as err:
                logger.error(f'compare error:{err}')
                pass

        if 'tasks' in choices:
            logger.info('updating tasks')
            try:
                update_tasks(self, projects, types=types)
            except Exception as err:
                logger.info(f'problem updating tasks, duplicate build:{err}')
                import traceback
                traceback.print_exc()

        if 'scans' in choices:
            logger.info('updating scans')
            update_scans(self, projects)

    def report(self, project, monthly=False):
        """Create a PDF report."""
        pdf_file = f'{project}_report.pdf'

        if os.path.exists(pdf_file):
            logger.info(f'{pdf_file} exists, delete or rename.')
            return

        logger.info(f'writing report to file:{pdf_file}.')
        make_project_report(
            self, project, pdf_file, monthly=monthly)

    def export_pdf(self, project, ptype):
        """Create a PDF report the merges all PDFs of this proc type."""
        pdf_file = f'{project}_{ptype}.pdf'

        if os.path.exists(pdf_file):
            logger.info(f'{pdf_file} exists, delete or rename.')
            return

        logger.info(f'TBD:writing report to file:{pdf_file}.')
        # TODO: make_proc_report(self, project, ptype, pdf_file)

    def export_stats(self, projects, proctypes, sesstypes, csvname, persubject=False, analysis=None, sessions=None):
        """Create a csv."""

        if os.path.exists(csvname):
            logger.info(f'{csvname} exists, delete or rename.')
            return

        logger.info(f'writing csv file:{csvname}.')
        make_stats_csv(
            self, projects, proctypes, sesstypes, csvname, persubject, analysis, sessions)

    def export_zip(self, filename, projects, proctypes, sesstypes, analysis=None, sessions=None):
        """Create a zip file of stats csv files."""

        if os.path.exists(filename):
            logger.info(f'{filename} exists, delete or rename.')
            return

        print(f'{filename=}')
        print(f'{projects=}')
        print(f'{proctypes=}')
        print(f'{sesstypes=}')
        print(f'{analysis=}')


        logger.info(f'writing zip file:{filename}.')
        make_export_zip(
            self, filename, projects, proctypes, sesstypes, sessions
        )

    def export_covariates(self, tmpdir, subj):
        '''Export covariates for subjects in subj to csv files in tmpdir'''
        return covariates_export(self, tmpdir, subj)

    def statshot(
        self,
        projects,
        proctypes=None,
        sesstypes=None,
        exclude=None,
        guid=False,
        ident=False
    ):
        """ Exports stats and save as new analysis on project."""
        make_statshot(
            self,
            projects,
            proctypes, 
            sesstypes,
            exclude,
            guid_filter=guid,
            id_filter=ident,
        )

    def anonshot(
        self,
        project,
        anonproject,
    ):
        """Export main project stats as new analysis on anon project."""
        make_anonshot(
            self,
            project,
            anonproject
        )

    def compare(self, project):
        """Create a PDF report of Double Entry Comparison."""
        pdf_file = f'{project}_double.pdf'
        excel_file = f'{project}_double.xlsx'

        if os.path.exists(pdf_file):
            logger.info(f'{pdf_file} exists, delete or rename.')
            return

        if os.path.exists(excel_file):
            logger.info(f'{excel_file} exists, delete or rename.')
            return

        logger.info(f'writing report to file:{pdf_file},{excel_file}.')
        # Get the projects to compare
        proj_primary = self.primary(project)
        proj_secondary = self.secondary(project)

        if not proj_secondary:
            logger.debug(f'cannot run, secondary REDCap not set:{project}')
            return

        if not proj_primary:
            logger.debug(f'cannot compare, primary REDCap not set:{project}')
            return

        make_double_report(proj_primary, proj_secondary, pdf_file, excel_file)

    def stats_projects(self):
        """List of projects that have stats, checks for a stats project ID."""
        def_field = self._rc.def_field

        if not self.redcap_enabled():
            logger.info('cannot load stats, redcap not enabled')
            return None

        _fields = [def_field, 'project_stats']
        rec = self._rc.export_records(fields=_fields)
        return [x[def_field] for x in rec if x['project_stats']]

    def add_task(self, project, assr, inputlist, var2val, walltime, memreq, yamlfile, userinputs):
        """Add a new task record ."""
        def_field = self._rcq.def_field

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
                    def_field: project,
                    'redcap_repeat_instrument': 'taskqueue',
                    'redcap_repeat_instance': task_id,
                    'task_status': 'QUEUED',
                    'task_inputlist': inputlist,
                    'task_var2val': var2val,
                    'task_walltime': walltime,
                    'task_memreq': memreq,
                    'task_yamlfile': task_yamlfile,
                    'task_userinputs': userinputs,
                    'task_timeused': '',
                    'task_memused': '',
                }
                response = self._rcq.import_records([record])
                assert 'count' in response
                logger.debug('task record created')
            except AssertionError as err:
                logger.error(f'upload failed:{err}')
                return
        else:
            # Create a new record
            try:
                record = {
                    def_field: project,
                    'redcap_repeat_instrument': 'taskqueue',
                    'redcap_repeat_instance': 'new',
                    'task_assessor': assr,
                    'task_status': 'QUEUED',
                    'task_inputlist': inputlist,
                    'task_var2val': var2val,
                    'task_walltime': walltime,
                    'task_memreq': memreq,
                    'task_yamlfile': task_yamlfile,
                    'task_userinputs': userinputs,
                }
                response = self._rcq.import_records([record])
                assert 'count' in response
                logger.debug('task record created')

            except AssertionError as err:
                logger.error(f'upload failed:{err}')
                return

        # If the file is not in yaml dir, we need to upload it to the task
        if task_yamlfile == 'CUSTOM':
            logger.debug(f'yaml not in shared library, uploading to task')
            if not task_id:
                # Try to match existing record
                task_id = self.assessor_task_id(project, assr)

            logger.debug(f'uploading file:{yamlfile}')
            utils_redcap.upload_file(
                self._rcq,
                project,
                'task_yamlupload',
                yamlfile,
                repeat_id=task_id)

    def assessor_task_id(self, project, assessor):
        task_id = None
        def_field = self._rcq.def_field

        if not self.redcap_enabled():
            logger.info('cannot load assessor task id, redcap not enabled')
            return None

        rec = self._rcq.export_records(
            forms=['taskqueue'],
            records=[project],
            fields=[def_field, 'task_assessor'])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'taskqueue']
        rec = [x for x in rec if x['task_assessor'] == assessor]

        if len(rec) > 1:
            logger.warn(f'duplicate tasks for assessor, not good:{assessor}')
            task_id = rec[0]['redcap_repeat_instance']
        elif len(rec) == 1:
            task_id = rec[0]['redcap_repeat_instance']

        return task_id

    def add_progress(self, project, prog_name, prog_date, prog_pdf, prog_zip):
        """Add a progress record with PDF and Zip at dated and named."""
        # Format for REDCap
        progress_datetime = prog_date.strftime("%Y-%m-%d %H:%M:%S")
        def_field = self._rc.def_field

        # Add new record
        try:
            record = {
                def_field: project,
                'progress_datetime': progress_datetime,
                'redcap_repeat_instrument': 'progress',
                'redcap_repeat_instance': 'new',
                'progress_name': prog_name,
                'progress_complete': '2',
            }
            response = self._rc.import_records([record])
            assert 'count' in response
            logger.debug('created new progress record')

            # Determine the new record id
            logger.debug('locating new record')
            _ids = utils_redcap.match_repeat(
                self._rc,
                project,
                'progress',
                'progress_datetime',
                progress_datetime)
            repeat_id = _ids[-1]

            # Upload output files
            logger.debug(f'uploading files to:{repeat_id}')
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
            logger.error(f'upload failed:{err}')
        except (ValueError, RedcapError) as err:
            logger.error(f'error uploading:{err}')

    def add_double(self, project, comp_name, comp_date, comp_pdf, comp_excel):
        """Add a compare record with PDF and Excel at dated and named."""
        def_field = self._rc.def_field

        # Format for REDCap
        compare_datetime = comp_date.strftime("%Y-%m-%d %H:%M:%S")

        # Add new record
        try:
            record = {
                def_field: project,
                'double_datetime': compare_datetime,
                'redcap_repeat_instrument': 'double',
                'redcap_repeat_instance': 'new',
                'double_name': comp_name,
                'double_complete': '2',
            }
            response = self._rc.import_records([record])
            assert 'count' in response
            logger.debug('double record created')

            # Determine the new record id
            logger.debug('locating new record')
            _ids = utils_redcap.match_repeat(
                self._rc,
                project,
                'double',
                'double_datetime',
                compare_datetime)
            repeat_id = _ids[-1]

            # Upload output files
            logger.debug(f'uploading files to:{repeat_id}')
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
            logger.error(f'upload failed:{err}')
        except (ValueError, RedcapError) as err:
            logger.error(f'error uploading:{err}')

    def get_sgp_source_stats(self, project, subject, assessor, stats_dir):
        """Download stats files to directory."""
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        resource = 'STATS'

        xpath = f'/projects/{project}/subjects/{subject}/experiments/{assessor}/resources/{resource}'

        logger.debug(f'selecting sgp:{xpath}')

        xnat_resource = self._xnat.select(xpath)
    
        xnat_resource.get(stats_dir, extract=True)

        return f'{stats_dir}/STATS'

    def get_source_stats(self, project, subject, session, assessor, stats_dir):
        """Download stats files to directory."""
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        resource = 'STATS'

        xnat_resource = self._xnat.select_assessor_resource(
            project,
            subject,
            session,
            assessor,
            resource)

        xnat_resource.get(stats_dir, extract=True)

        return f'{stats_dir}/STATS'

    def get_scan_stats(self, project, subject, session, scan):
        """Read from BIDS file."""
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        stats = {}
        resource = 'JSON'

        xnat_resource = self._xnat.select_scan_resource(
            project,
            subject,
            session,
            scan,
            resource)

        # Get list of files in JSON resource
        files = xnat_resource.files().get()
        if len(files) == 0:
            logging.debug(f'no JSON files found')
        elif len(files) > 1:
            logging.debug(f'too many JSON files found')
        else:
            # Download and load the first file
            src = files[0]
            with tempfile.TemporaryDirectory() as tmpdir:
                dst = os.path.join(tmpdir, src)
                xnat_resource.file(src).get(dst)
                stats.update(self._load_bids_data(dst))

        return stats

    def set_stats(self, project, subject, session, assessor, data):
        """Upload stats to redcap."""

        if 'Multi_Atlas_v3' in assessor:
            data = {k: data.get(k, '') for k in ['ticv_mm3']}

        if len(data.keys()) > self.max_stats:
            logger.debug('found too many, specify subset')
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
        logger.debug(f'uploading to redcap:{project}')
        statsrc = self._stats_redcap(project)
        logger.debug(f'{statsrc=}')

        try:
            logger.debug('importing records')
            response = statsrc.import_records(rec)
            assert 'count' in response
            logger.debug('stats record created')
        except AssertionError as err:
            logger.error(f'upload failed:{err}')
        except ConnectionError as err:
            logger.error(err)
            logger.info('wait a minute')
            import time
            time.sleep(60)

    def set_sgp_stats(self, project, subject, assessor, data):
        """Upload stats to redcap."""

        if len(data.keys()) > self.max_stats:
            logger.debug('found too many, specify subset')
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
        logger.debug(f'uploading to redcap:{project}')
        statsrc = self._stats_redcap(project)
        logger.debug(f'{statsrc=}')

        try:
            logger.debug('importing records')
            response = statsrc.import_records(rec)
            assert 'count' in response
            logger.debug('stats record created')
        except AssertionError as err:
            logger.error(f'upload failed:{err}')
        except ConnectionError as err:
            logger.error(err)
            logger.info('wait a minute')
            import time
            time.sleep(60)


    def set_scan_stats(self, project, subject, session, scan, data):
        """Upload to redcap."""

        rec = {
            'subject_id': subject,
            'scan_session': session,
            'scan_id': scan,
            'redcap_repeat_instrument': 'scan',
            'redcap_repeat_instance': 'new',
            'scan_complete': '2',
            'scan_duration': data['duration'],
            'scan_tr': data['tr'],
            'scan_prfip': data['prfip'],
            'scan_proop': data['proop'],
            'scan_slicethickness':data['thickness']
        }

        # Now upload
        try:
            logger.debug('uploading to redcap')
            response = self._stats_redcap(project).import_records([rec])
            assert 'count' in response
            logger.debug('scan record uploaded')
        except AssertionError as err:
            logger.error(f'upload failed:{err}')
        except ConnectionError as err:
            logger.error(err)
            logger.info('wait a minute')
            import time
            time.sleep(60)

    def project_setting(self, project, setting):
        """Return the value of the setting for this project."""

        if not self.redcap_enabled():
            logger.info('cannot load project setting, redcap not enabled')
            return None

        if isinstance(project, list):
            return self.projects_setting(project, setting)

        records = self._rc.export_records(records=[project], forms=['main'])
        if not records:
            return None

        # First try "project" then try "main"
        rec = records[0]
        return rec.get(f'project_{setting}', rec.get(f'main_{setting}', None))

    def projects_setting(self, projects, setting):
        """Return the value of the setting for projects as dict."""
        project2setting = {}
        def_field = self._rc.def_field

        if not self.redcap_enabled():
            logger.info('cannot load project setting, redcap not enabled')
            return None

        records = self._rc.export_records(records=projects, forms=['main'])
        if not records:
            return None

        for rec in records:
            # First try "project" then try "main" otherwise nothing
            #project2setting[self._dfield()] = rec.get(
            #    f'project_{setting}', rec.get(f'main_{setting}', None))
            project2setting[rec[def_field]] = rec.get(
                f'project_{setting}', rec.get(f'main_{setting}', None))

        return project2setting

    def etl_automations(self, project):
        """Get ETL automation records."""
        etl_autos = []

        if not self.redcap_enabled():
            logger.info('cannot load etl_automations, redcap not enabled')
            return []

        auto_names = self.etl_automation_choices()
        logger.debug(f'loading etl_automations:{project}')
        try:
            rec = self._rc.export_records(records=[project], forms=['main'])[0]
        except IndexError:
            return []

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

    def edat_automation_choices(self):
        """Get the names of the automations, checkboxes in REDCap."""
        names = None

        for x in self._rc.metadata:
            # dcm2niix, dcm2niix | xnat_auto_archive, xnat_auto_archive
            if x['field_name'] == 'edat_autos':
                names = x['select_choices_or_calculations']
                names = [x for x in names.split('|')]
                names = [n.split(',')[0].strip() for n in names]

        return names

    def scan_automations(self, project):
        """Get scanning automation records."""
        scan_autos = []

        if not self.redcap_enabled():
            logger.info('cannot load scan automations, redcap not enabled')
            return []

        auto_names = self.scan_automation_choices()

        try:
            rec = self._rc.export_records(records=[project], forms=['main'])[0]
        except IndexError:
            return []

        # Determine what scan autos we want to run
        for a in auto_names:
            if rec.get(f'main_scanautos___{a}', '') == '1':
                scan_autos.append(a)

        return scan_autos

    def edat_protocols(self, project):
        """Return list of edat protocol records."""

        if not self.redcap_enabled():
            logger.info('cannot load edat protocols, redcap not enabled')
            return []

        rec = self._rc.export_records(records=[project], forms=['edat'])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'edat']

        return rec

    def scanning_protocols(self, project):
        """Return list of scanning protocol records."""

        if not self.redcap_enabled():
            logger.info('cannot load scanning protocols, redcap not enabled')
            return []

        rec = self._rc.export_records(records=[project], forms=['scanning'])

        # this will remove the main record that is sometimes included
        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'scanning']

        return rec

    def add_issues(self, issues):
        """Add list of issues."""
        records = []
        issue_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        def_field = self._rc.def_field

        for i in issues:
            records.append({
                def_field: i['project'],
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
            logger.debug(records)
            response = self._rc.import_records(records)
            assert 'count' in response
            logger.debug('issues uploaded')
        except AssertionError as err:
            logger.error(f'issues upload failed:{err}')

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
        def_field = self._rc.def_field

        record = {
            def_field: project,
            'issue_description': description,
            'issue_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
            logger.debug('issue record created')
        except (ValueError, RedcapError, AssertionError) as err:
            logger.error(f'error uploading:{err}')

    def primary(self, project):
        """Connect to the primary redcap for this project."""
        primary_redcap = None
        project_id = self.project_setting(project, 'primary')
        if not project_id:
            logger.debug(f'no primary project id found:{project}')
            return None

        try:
            primary_redcap = utils_redcap.get_redcap(project_id)
        except Exception as err:
            logger.debug(f'could not load primary redcap:{project}:{err}')
            primary_redcap = None

        return primary_redcap

    def secondary(self, project):
        """Connect to the secondary redcap for this project."""
        secondary_redcap = None
        project_id = self.project_setting(project, 'secondary')
        if not project_id:
            logger.debug(f'no secondary project id found:{project}')
            return None

        try:
            secondary_redcap = utils_redcap.get_redcap(project_id)
        except Exception as err:
            logger.info(f'failed to load secondary redcap:{project}:{err}')
            secondary_redcap = None

        return secondary_redcap

    def identifier_database(self):
        """Connect to the identifier database redcap for this project."""
        identifier_redcap = None

        try:
            identifier_redcap = utils_redcap.get_identifier_redcap()
        except Exception as err:
            logger.info(f'failed to load identifier database redcap:{project}:{err}')
            identifier_redcap = None

        return identifier_redcap

    def alternate(self, project_id):
        """Connect to the alternate redcap with this ID."""
        alt_redcap = None

        try:
            alt_redcap = utils_redcap.get_redcap(project_id)
        except Exception as err:
            logger.info(f'failed to load alternate redcap:{project_id}:{err}')
            alt_redcap = None

        return alt_redcap

    def xnat(self):
        """Get the xnat for this garjus."""
        return self._xnat

    def xnat_host(self):
        if self.xnat_enabled():
            return self._xnat.host
        else:
            return ''

    def redcap(self):
        """Get the redcap project for this garjus."""
        return self._rc

    def redcap_host(self):
        """Get the redcap host for this garjus."""
        return self._rc.url

    def redcap_pid(self):
        """Get the redcap host for this garjus."""
        return str(self._rc.export_project_info().get('project_id'))

    def rcq_pid(self):
        """Get the redcap host for this garjus."""
        return str(self._rcq.export_project_info().get('project_id'))

    def redcap_url(self):
        return self._rcq.url

    def redcap_version(self):
        return str(self._rcq.redcap_version)

    def get_link(self, instrument, project_id, repeat_id):
        redcap_url = self.redcap_url()
        redcap_version = self.redcap_version()

        if instrument in ['taskqueue', 'processing', 'analyses']:
            redcap_pid = self.rcq_pid()
        else:
            redcap_pid = self.redcap_pid()

        if redcap_url.endswith('/api/'):
            redcap_url = redcap_url[:-5]

        redcap_link = f'{redcap_url}/redcap_v{redcap_version}/DataEntry/index.php?pid={redcap_pid}&page={instrument}&id={project_id}&instance={repeat_id}'

        return redcap_link

    def _copy_session(
        self,
        src_proj,
        src_subj,
        src_sess,
        dst_proj,
        dst_subj,
        dst_sess
    ):
        """Copy scanning/imaging session from source to destination."""
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        src_obj = self._xnat.select_session(src_proj, src_subj, src_sess)
        dst_obj = self._xnat.select_session(dst_proj, dst_subj, dst_sess)
        utils_xnat.copy_session(src_obj, dst_obj)

    def _copy_scan(
        self,
        src_proj,
        src_subj,
        src_sess,
        src_scan,
        dst_proj,
        dst_subj,
        dst_sess,
        dst_scan,
    ):
        """Copy scanning/imaging scan from source to destination."""
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        src_obj = self._xnat.select_scan(
            src_proj, src_subj, src_sess, src_scan)
        dst_obj = self._xnat.select_scan(
            dst_proj, dst_subj, dst_sess, dst_scan)
        utils_xnat.copy_scan(src_obj, dst_obj)

    def source_project_exists(self, project):
        """True if this project exist in the source projects."""
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        return self._xnat.select.project(project).exists()

    def project_exists(self, project):
        """True if this project exists."""
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        xnat_exists = self._xnat.select.project(project).exists()
        return xnat_exists

    def close_issues(self, issues):
        """Close specified issues, set to complete in REDCap."""
        records = []
        issue_closedate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        def_field = self._rc.def_field

        for i in issues:
            records.append({
                def_field: i['project'],
                'redcap_repeat_instance': i['id'],
                'issue_closedate': issue_closedate,
                'redcap_repeat_instrument': 'issues',
                'issues_complete': 2,
            })

        try:
            response = self._rc.import_records(records)
            assert 'count' in response
            logger.debug('issues records completed')
        except AssertionError as err:
            logger.error(f'failed to set issues to complete:{err}')

    def delete_issues(self, issues):
        """Delete specified issues, delete in REDCap."""
        def_field = self._rc.def_field

        try:
            for i in issues:
                _main = i[def_field]
                _id = i['redcap_repeat_instance']
                logger.debug(f'deleting:issue:{_main}:{_id}')
                # /api/help/?content=del_records
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
            logger.error(f'failed to delete records:{err}')

    def delete_tasks(self, tasks):
        """Delete in REDCap."""
        def_field = self._rcq.def_field
        instrument = 'taskqueue'
        token = self._rcq.token

        try:
            for i in tasks:
                _main = i[def_field]
                _id = i['redcap_repeat_instance']
                logger.debug(f'deleting:{instrument}:{_main}:{_id}')
                # /api/help/?content=del_records
                _payload = {
                    'action': 'delete',
                    'returnFormat': 'json',
                    'records[0]': _main,
                    'instrument': instrument,
                    'repeat_instance': _id,
                    'content': 'record',
                    'token': token,
                    'format': 'json'}

                self._rcq._call_api(_payload, 'del_record')
        except Exception as err:
            logger.error(f'failed to delete records:{err}')

    def rename_dicom(self, in_dir, out_dir):
        """Sort DICOM folder into scans."""
        utils_dcm2nii.rename_dicom(in_dir, out_dir)

    def _load_json_info(self, jsonfile):
        with open(jsonfile) as f:
            data = json.load(f, strict=False)

        return {
            'modality': data.get('Modality', None),
            'date': data.get('AcquisitionDateTime', None),
            'tracer': data.get('Radiopharmaceutical', None),
            'ProcedureStepDescription': data.get('ProcedureStepDescription', None),
        }

    def _load_bids_data(self, jsonfile):
        with open(jsonfile) as f:
            data = json.load(f, strict=False)

        return {
            'modality': data.get('Modality', ''),
            'date': data.get('AcquisitionDateTime', ''),
            'tracer': data.get('Radiopharmaceutical', ''),
            'ProcedureStepDescription': data.get('ProcedureStepDescription', ''),
            'duration': str(data.get('AcquisitionDuration', '')),
            'thickness': str(data.get('SliceThickness', '')),
            'tr': str(data.get('RepetitionTime', '')),
            'te': str(data.get('EchoTime', '')), 
            'flip': str(data.get('FlipAngle', '')),
            'prfip': str(data.get('ParallelReductionFactorInPlane', '')),
            'proop': str(data.get('ParallelReductionOutOfPlane', data.get('ParallelReductionFactorOutOfPlane', ''))),
            'manufact': str(data.get('Manufacturer', '')),
            'dcm2niix': str(data.get('ConversionSoftwareVersion', '')),
        }

    def _upload_scan(self, dicomdir, scan_object):
        nifti_list = []
        bval_path = ''
        bvec_path = ''
        json_path = ''

        # check that it hasn't been converted yet
        nifti_count = len(glob.glob(os.path.join(dicomdir, '*.nii.gz')))
        if nifti_count > 0:
            logger.info(f'nifti exists:{dicomdir}')
            return None

        # convert
        niftis = utils_dcm2nii.dicom2nifti(dicomdir)
        if not niftis:
            logger.info(f'nothing converted:{dicomdir}')
            return None

        # if session needs to be created, get the attributes from the scan json
        jsonfile = glob.glob(os.path.join(dicomdir, '*.json'))[0]

        # load json data from file
        scan_info = self._load_json_info(jsonfile)

        # Truncate datetime
        if scan_info['date']:
            scan_info['date'] = scan_info['date'][:10]

        scan_modality = scan_info['modality']
        scan_tracer = scan_info['tracer']
        scan_date = scan_info['date']
        if scan_date == '1969-12-31':
            scan_date = ''

        if scan_modality == 'MR':
            sess_datatype = 'xnat:mrSessionData'
            scan_datatype = 'xnat:mrScanData'
        elif scan_modality == 'PT':
            sess_datatype = 'xnat:petSessionData'
            scan_datatype = 'xnat:petScanData'
        elif scan_modality == 'CT':
            sess_datatype = 'xnat:petSessionData'
            scan_datatype = 'xnat:ctScanData'
        elif scan_info['ProcedureStepDescription'] == 'C-11_PiB':
            sess_datatype = 'xnat:petSessionData'
            scan_datatype = 'xnat:petScanData'
            scan_modality = 'PET'
            scan_tracer = 'PiB C-11'
        else:
            logger.info(f'unsupported modality:{scan_modality}')
            return

        if not scan_object.parent().exists():
            # create session with date, modality
            logger.debug(f'creating xnat session:type={sess_datatype}')
            scan_object.parent().create(experiments=sess_datatype)
            if scan_date:
                logger.debug(f'set date={scan_date}')
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
            logger.debug(f'set tracer:{scan_tracer}')
            scan_object.parent().attrs.set('tracer_name', scan_tracer)

        if not scan_object.exists():
            logger.debug(f'creating xnat scan:datatype={scan_datatype}')

            # make the scan
            scan_object.create(scans=scan_datatype)
            scan_object.attrs.mset(scan_attrs)

        elif scan_object.resource('DICOMZIP').exists():
            logger.debug('skipping, DICOMZIP already exists')
            return

        # upload the converted files, NIFTI/JSON/BVAL/BVEC
        for fpath in glob.glob(os.path.join(dicomdir, '*')):
            if not os.path.isfile(fpath):
                continue

            if fpath.endswith('ADC.nii.gz'):
                logger.debug(f'ignoring ADC NIFTI:{fpath}')
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
            logger.info('dcm2nii:multiple NIFTI')

        # Upload the dicom zip
        utils_xnat.upload_dirzip(dicomdir, scan_object.resource('DICOMZIP'))

        # Upload the NIFTIs
        utils_xnat.upload_files(nifti_list, scan_object.resource('NIFTI'))

        if os.path.isfile(bval_path) and os.path.isfile(bvec_path):
            logger.info('uploading BVAL/BVEC')
            utils_xnat.upload_file(bval_path, scan_object.resource('BVAL'))
            utils_xnat.upload_file(bvec_path, scan_object.resource('BVEC'))

        if os.path.isfile(json_path):
            logger.info(f'uploading JSON:{json_path}')
            utils_xnat.upload_file(json_path, scan_object.resource('JSON'))

    def _upload_nifti(self, nifti, scan_object, scan_type, data_type):
        scan_attrs = {
            'series_description': scan_type,
            'type': scan_type,
            'quality': 'usable'}

        # make the scan
        logger.debug(f'creating xnat scan:datatype={data_type}')
        scan_object.create(scans=data_type)
        scan_object.attrs.mset(scan_attrs)

        # Upload the NIFTIs
        utils_xnat.upload_files([nifti], scan_object.resource('NIFTI'))

    def _upload_edat(self, edat_file, scan_object):
        utils_xnat.upload_files([edat_file], scan_object.resource('EDAT'))

    def upload_session(self, session_dir, project, subject, session):
        # session dir - should only contain a subfolder for each series
        # as created by rename_dicom()
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        session_exists = False

        # Check that project exists
        if not self._xnat.select_project(project).exists():
            logger.info('project does not exist, refusing to create')
            return

        # Check that subject exists, create as needed
        subject_object = self._xnat.select_subject(project, subject)
        if not subject_object.exists():
            logger.info(f'subject does not exist, creating:{subject}')
            subject_object.create()
        else:
            logger.info(f'subject exists:{subject}')

        session_object = subject_object.experiment(session)
        if not session_object.exists():
            logger.info(f'session does not exist, will be created later')
            # wait until get have attributes from json file: date, modality
        else:
            logger.info(f'session exists:{session}')
            session_exists = True

        # Handle each scan
        for p in sorted(pathlib.Path(session_dir).iterdir()):
            if not p.is_dir():
                # Ignore non-directories
                continue

            scan = p.name
            scan_object = session_object.scan(scan)

            if session_exists and scan_object.exists():
                logger.info(f'scan exists, skipping:{scan}')
                continue

            logger.info(f'uploading scan:{scan}')
            self._upload_scan(p, scan_object)
            logger.info(f'finished uploading scan:{scan}')

    def upload_scan(self, scan_dir, project, subject, session, scan):
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        # Check that subject exists
        subject_object = self._xnat.select_subject(project, subject)
        if not subject_object.exists():
            logger.info(f'subject does not exist:{subject}')
            return

        # Check that session exists
        session_object = subject_object.experiment(session)
        if not session_object.exists():
            logger.info(f'session does not exist')
            return

        scan_object = session_object.scan(scan)
        if scan_object.exists():
            logger.info(f'scan exists, skipping:{scan}')
            return

        logger.info(f'uploading scan:{scan}')
        self._upload_scan(scan_dir, scan_object)
        logger.info(f'finished uploading scan:{scan}')

    def upload_nifti(self, nifti, project, subject, session, scan, scantype, modality='MR'):
        if modality == 'PET':
            sess_datatype = 'xnat:petSessionData'
        else:
            sess_datatype = 'xnat:mrSessionData'

        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        # Check that subject exists, create as needed
        subject_object = self._xnat.select_subject(project, subject)
        if not subject_object.exists():
            logger.info(f'subject does not exist, creating:{subject}')
            subject_object.create()
        else:
            logger.info(f'subject exists:{subject}')

        session_object = subject_object.experiment(session)
        if not session_object.exists():
            logger.info(f'session does not exist, creating')
            session_object.create(experiments=sess_datatype)
        else:
            logger.info(f'session exists:{session}')

        scan_object = session_object.scan(scan)
        
        if scan_object.exists():
            logger.info(f'scan exists, skipping:{scan}')
            return

        logger.info(f'uploading nifti:{nifti}')
        if modality == 'PET':
            scan_datatype = 'xnat:petScanData'
        else:
            scan_datatype = 'xnat:mrScanData'
        self._upload_nifti(nifti, scan_object, scantype, scan_datatype)
        logger.info(f'finished uploading nifti:{nifti}')

    def upload_edat(self, edat_file, project, subject, session, scan):
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        # Check that subject exists, create as needed
        subject_object = self._xnat.select_subject(project, subject)
        if not subject_object.exists():
            logger.info(f'subject does not exist:{subject}')
            raise Exception(f'subject does not exist on xnat')

        session_object = subject_object.experiment(session)
        if not session_object.exists():
            logger.info(f'session does not exist')
            raise Exception(f'session does not exist on xnat')

        scan_object = session_object.scan(scan)
        if not scan_object.exists():
            logger.info(f'scan does not exist')
            raise Exception(f'scan does not exist on xnat')

        logger.info(f'uploading edat:{edat_file}')
        self._upload_edat(edat_file, scan_object)
        logger.info(f'finished uploading edat:{edat_file}')

    def import_dicom_xnat(self, src, proj, subj, sess):
        if not self.xnat_enabled():
            raise Exception('xnat not enabled')

        with tempfile.TemporaryDirectory() as temp_dir:

            # Download all inputs
            if src.count('/') == 3:
                # Download specified scan
                s_proj, s_subj, s_sess, s_scan = src.split('/')
                logger.info(f'download DICOM:{s_proj}:{s_sess}:{s_scan}')
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

                    logger.info(f'download DICOM:{s_proj}:{s_sess}:{s_scan}')
                    scan.resource('DICOM').get(temp_dir, extract=True)

            # Upload them
            logger.info(f'uploading session:{temp_dir}:{proj}:{subj}:{sess}')
            import_dicom_dir(self, temp_dir, proj, subj, sess)

    def image03csv(self, project, startdate=None, enddate=None, sites=None):
        update_image03(self, [project], startdate, enddate, sites)

    def image03download(self, project, image03_csv, download_dir):
        download_image03(self, project, image03_csv, download_dir)

    def run_analysis(self, project, subjects, repo, jobdir, csv, yamlfile, imagedir, exclude, reuse_inputs=False):
        if subjects is not None and not isinstance(subjects, list):
            subjects = subjects.split(',')

        if exclude is not None and not isinstance(exclude, list):
            exclude = exclude.split(',')
    
        run_analysis(self, project, subjects, repo, jobdir, csv, yamlfile, imagedir, exclude, reuse_inputs=reuse_inputs)


    def run_spin(self, project, subject, jobdir, yamlfile, imagedir, reuse_inputs=False):
        run_spin(self, project, subject, jobdir, yamlfile, imagedir, reuse_inputs=reuse_inputs)

    def download_proctype(self, project, download_dir, proctype, resources, files, sesstypes=None, analysis_id=None, sessinclude=None):
        download_resources(self, project, download_dir, proctype, resources, files, sesstypes, analysis_id, sessinclude)

    def download_scantype(self, project, download_dir, scantype, resources, files, sesstypes=None, sessinclude=None):
        download_scan_resources(self, project, download_dir, scantype, resources, files, sesstypes, sessinclude)

    # Pass tasks from garjus to dax by writing files to DISKQ
    def queue2dax(self):
        from .tasks import garjus2dax
        # TODO: check for duplicate inputs
        garjus2dax.queue2dax(self)

    # Update queue from dax
    def dax2queue(self):
        from .tasks import dax2garjus
        dax2garjus.dax2queue(self)

    # Check for duplicate build
    def detect_duplicate(self, project_data):
        detected = False

        logger.debug('checking for duplicate build')

        # Get current tasks
        logger.debug('load tasks')
        df = self.tasks(
            download=False,
            hidedone=True,
            projects=[project_data['name']])

        # Check for any newly created assessors that we didn't create
        logger.debug('compare')
        df = df[df.PROJECT == project_data['name']]
        df = df[~df.ASSESSOR.isin(list(project_data['assessors'].ASSR))]
        df = df[~df.ASSESSOR.isin(self.our_assessors())]

        if not df.empty:
            detected = True

        return detected

    def retry(self, project, proctype=None):
        '''Delete outputs on xnat, set to job running, reset on redcap'''
        SKIP_LIST = ['OLD', 'EDITS']
        records = []
        def_field = self._rcq.def_field

        # get tasks with status of fail, failcount blank or 0
        df = self.tasks(hidedone=False)
        df = df[df.PROJECT == project]
        failed_tasks = df[(df.STATUS == 'JOB_FAILED') & (df.FAILCOUNT == '')]

        if len(failed_tasks) == 0:
            logger.info('no failed tasks found')
            return

        logger.debug(f'deleting files from failed tasks:{len(failed_tasks)}')

        dfa = self.assessors(projects=[project])
        dfs = self.subject_assessors(projects=[project])

        for i, t in failed_tasks.iterrows():
            assr = t['ASSESSOR']
            is_sgp = is_sgp_assessor(t['ASSESSOR'])

            if proctype:
                if is_sgp:
                    if assr.split('-x-')[2] != proctype:
                        continue
                else:
                    if assr.split('-x-')[3] != proctype:
                        continue

            # Connect to the assessor on xnat
            if is_sgp:
                xsitype = 'proc:subjgenprocdata'
                _subj = assr.split('-x-')[1]
                assessor = self.xnat().select(f'/projects/{project}/subjects/{_subj}/experiment/{assr}')
            else:
                xsitype = 'proc:genprocdata'
                assessor = self.xnat().select_assessor(
                    project,
                    assr.split('-x-')[1],
                    assr.split('-x-')[2],
                    assr)

            if not assessor.exists():
                logger.debug(f'assessor not found on xnat:{assr}')
                continue

            # Check for already cleared
            try:
                if is_sgp:
                    _status = list(dfs[dfs.ASSR == assr].QCSTATUS).pop()
                else:
                    _status = list(dfa[dfa.ASSR == assr].QCSTATUS).pop()
            except Exception as err:
                logger.info(f'get status from xnat:{assr}')
                _status = assessor.attrs.get(f'{xsitype}/validation/status')

            if _status == 'Job Pending':
                logger.info(f'already cleared xnat:{assr}')
            else:
                # Clear previous results
                logger.info(f'clearing xnat attributes:{assr}')
                assessor.attrs.mset({
                    f'{xsitype}/validation/status': 'Job Pending',
                    f'{xsitype}/jobid': ' ',
                    f'{xsitype}/memused': ' ',
                    f'{xsitype}/walltimeused': ' ',
                    f'{xsitype}/jobnode': ' ',
                })

                resources = assessor.resources()
                resources = [x for x in resources if x.label() not in SKIP_LIST]
                for res in resources:
                    logger.info(f'deleting xnat resource:{assr}:{res.label()}')
                    try:
                        res.delete()
                    except Exception:
                        logger.error('deleting xnat resource')

            # Append new record for redcap
            records.append({
                def_field: project,
                'redcap_repeat_instrument': 'taskqueue',
                'redcap_repeat_instance': t['ID'],
                'task_status': 'QUEUED',
                'task_timeused': '',
                'task_memused': '',
                'task_failcount': '1',
            })

        for i, t in failed_tasks.iterrows():
            assr = t['ASSESSOR']

            if proctype:
                if is_sgp_assessor(t['ASSESSOR']):
                    if assr.split('-x-')[2] != proctype:
                        continue
                else:
                    if assr.split('-x-')[3] != proctype:
                        continue

            # Connect to the assessor on xnat
            if is_sgp_assessor(t['ASSESSOR']):
                xsitype = 'proc:subjgenprocdata'
                _subj = assr.split('-x-')[1]
                assessor = self.xnat().select(f'/projects/{project}/subjects/{_subj}/experiment/{assr}')
            else:
                xsitype = 'proc:genprocdata'
                assessor = self.xnat().select_assessor(
                    project,
                    assr.split('-x-')[1],
                    assr.split('-x-')[2],
                    assr)

            if not assessor.exists():
                logger.debug(f'assessor not found on xnat:{assr}')
                continue

            logger.debug('set xnat procstatus to JOB_RUNNING')
            assessor.attrs.mset({
                f'{xsitype}/procstatus': 'JOB_RUNNING',
                f'{xsitype}/jobstartdate': str(date.today()),
            })

        if records:
            # Apply the updates in one call
            try:
                response = self._rcq.import_records(records)
                assert 'count' in response
                logger.debug('retry task records updated')
            except AssertionError as err:
                logger.error(f'failed to set task statuses:{err}')
        else:
            logger.debug('retry, nothing to update')

    def session_assessor_labels(self, project, subject, session):
        """Return list of labels."""
        uri = f'/REST/projects/{project}/subjects/{subject}/experiments/{session}/assessors?columns=label,xsiType'
        result = self._get_result(uri)
        result = [x for x in result if x['xsiType'] != 'fs:fsData']
        label_list = [x['label'] for x in result]
        return label_list

    def add_our_assessor(self, assessor):
        self._our_assessors.add(assessor)

    def our_assessors(self):
        return list(self._our_assessors)

    def load_linked(self, project, delete_dates=False):
        links = pd.DataFrame()
        rc_pre = self.project_setting(project, 'preanon')
        if not rc_pre:
            raise Exception(f'no preanon setting:{project=}')

        rc_anon = self.project_setting(project, 'anon')
        if not rc_anon:
            raise Exception(f'no anon setting:{project=}')

        rc_pre = utils_redcap.get_redcap(project_id=rc_pre)
        rc_anon = utils_redcap.get_redcap(project_id=rc_anon)

        return utils_redcap.load_link(rc_pre, rc_anon, delete_dates)

    def anonymize(self, project, in_dir, out_dir, delete_dates=False):
        links = self.load_linked(project, delete_dates=delete_dates)
        anonymize_project(in_dir, out_dir, links, delete_dates=delete_dates)

    def check_anonymize(self, project, out_dir):
        links = self.load_linked(project)
        check_project(out_dir, links)


def is_sgp_assessor(assessor):
    import re
    SGP_PATTERN = '^\w+-x-\w+-x-\w+_v[0-9]+-x-[0-9a-f]+$'

    # Try to match the assessor label with the SGP pattern
    return re.match(SGP_PATTERN, assessor)


def _subject_pivot(df):
    # Pivot to one row per subject
    level_cols = ['SESSTYPE', 'PROCTYPE']
    stat_cols = []
    index_cols = ['PROJECT', 'SUBJECT', 'SITE']

    # Drop any duplicates found
    df = df.drop_duplicates()

    # And duplicate proctype for session
    df = df.drop_duplicates(
        subset=['SUBJECT', 'SESSTYPE', 'PROCTYPE'],
        keep='last')

    df = df.drop(columns=['ASSR', 'SESSION', 'DATE'])

    stat_cols = [x for x in df.columns if (x not in index_cols and x not in level_cols)]

    # Make the pivot table based on _index, _cols, _vars
    dfp = df.pivot(index=index_cols, columns=level_cols, values=stat_cols)

    if len(df.SESSTYPE.unique()) > 1:
        # Concatenate column levels to get one level with delimiter
        dfp.columns = [f'{c[1]}_{c[0]}' for c in dfp.columns.values]
    else:
        dfp.columns = [c[0] for c in dfp.columns.values]

    # Clear the index so all columns are named
    dfp = dfp.dropna(axis=1, how='all')
    dfp = dfp.reset_index()

    return dfp


def _get_msit(df):
    rois = ['amyg', 'antins', 'ba46', 'bnst', 'dacc', 'lhpostins', 'pcc', 'pvn', 'rhpostins', 'sgacc', 'vmpfc']

    for r in rois:
        # Incongruent minus Congruent
        df[r] = (df['inc_' + r + '_mean'].astype('float') - df['con_' + r + '_mean'].astype('float')).round(6)

    # Averge postins
    df['postins'] = ((df['lhpostins'] + df['rhpostins']) / 2).round(6)

    return df


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    g = Garjus()
    print(g.projects())
    print(g.scans())
    print(g.assessors())
