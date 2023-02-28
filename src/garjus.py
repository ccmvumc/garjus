"""main garjus class.

All interactions with REDCap and XNAT should go through the main Garjus class.
Anything outside this class must refer to scans, assessors, issues, autos, etc.

To create a new main REDCap project:
-upload from zip
-click user rights, enable API export/import, save changes
-refresh, click API, clcik Generate API Token, click Copy
-go to ~/.redcap.txt
-paste key, copy & paste PID from gui, name main

To create a new stats REDCap project:
-copy an existing project in gui under Other Functionality, click Copy Project
-change the project name, check or uncheck
-click Copy Project (should take you to new project)
-click user rights, enable API export/import, save changes
-refresh, click API, clcik Generate API Token, click Copy
-go to ~/.redcap.txt
-paste key, copy & paste ID, name main
-paste ID into ccmutils under Main > Project Stats

To add a new primary REDCap project:
-Copy PID, key to ~/.redcap.txt, name PROJECT primary
-paste ID into ccmutils under Main > Project P

To add a new secondary REDCap project for double entry comparison:
-Copy PID, key to ~/.redcap.txt, name PROJECT secondary
-paste ID into ccmutils under Main > Project Secondary
"""
from typing import Optional
import logging
import json
from datetime import datetime

import requests
import pandas as pd
from redcap import Project, RedcapError
from pyxnat import Interface

from . import utils_redcap
from . import utils_xnat
from .progress import update as update_progress
from .stats import update as update_stats
from .automations import update as update_automations


COLUMNS = {
    'activity': ['PROJECT', 'SUBJECT', 'SESSION', 'SCAN', 'ID', 'DESCRIPTION', 'DATETIME', 'EVENT', 'FIELD', 'CATEGORY', 'RESULT', 'STATUS'],
    'assessors': ['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'DATE', 'SITE', 'ASSR', 'PROCSTATUS', 'PROCTYPE', 'JOBDATE', 'QCSTATUS', 'QCDATE', 'QCBY', 'XSITYPE', 'INPUTS', 'MODALITY'],
    'issues': ['PROJECT', 'SUBJECT', 'SESSION', 'ID', 'DESCRIPTION', 'DATETIME', 'EVENT', 'FIELD',  'CATEGORY', 'STATUS'],
    'scans': ['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'TRACER', 'DATE', 'SITE', 'SCANID', 'SCANTYPE', 'QUALITY', 'RESOURCES', 'MODALITY'],
}


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

    def __init__(self, redcap_project: Project=None, xnat_interface: Interface=None):
        """Initialize garjus."""
        if redcap_project:
            self._rc = redcap_project
        else:
            self._rc = self._default_redcap()

        if xnat_interface:
            self._xnat = xnat_interface
        else:
            self._xnat = self._default_xnat()

        self.scan_uri = utils_xnat.SCAN_URI
        self.assr_uri = utils_xnat.ASSR_URI
        self.scan_rename = utils_xnat.SCAN_RENAME
        self.assr_rename = utils_xnat.ASSR_RENAME
        self.activity_rename = utils_redcap.ACTIVITY_RENAME
        self.issues_rename = utils_redcap.ISSUES_RENAME
        self.xsi2mod = utils_xnat.XSI2MOD
        self.max_stats = 60
        self._projects = self._load_project_names()
        self._project2stats = {}
        self._columns = self._default_column_names()

    @staticmethod
    def _default_xnat():
        from dax.XnatUtils import get_interface
        return get_interface()

    @staticmethod
    def _default_redcap():
        from .utils_redcap import get_main_redcap
        return get_main_redcap()

    def activity(self, project):
        """List of activity records."""
        data = []

        # TODO: allow a date range filter

        rec = self._rc.export_records(
            records=[project],
            forms=['activity'],
            fields=[self._dfield()])

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'activity']
        for r in rec:
            d = {'PROJECT': r[self._dfield()], 'STATUS': 'COMPLETE'}
            for k, v in self.activity_rename.items():
                d[v] = r.get(k, '')

            data.append(d)

        return pd.DataFrame(data, columns=self.column_names('activity'))


    def add_activity(
        self,
        category,
        description,
        project=None,
        subject=None,
        event=None,
        session=None,
        field=None,
        actdatetime=None):
        """Add an activity record."""

        if not actdatetime:
            actdatetime =  datetime.now()

        # Format for REDCap
        activity_datetime = actdatetime.strftime("%Y-%m-%d %H:%M:%S")

        record = {
            self._dfield(): project,
            'activity_description': description,
            'activity_datetime': activity_datetime,
            'activity_event': event,
            'activity_field': field,
            'activity_result': 'COMPLETE',
            'activity_subject': subject,
            'activity_session': session,
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

    def column_names(self, datatype):
        """Return list of colum names for this data type."""
        return self._columns.get(datatype)

    def issues(self, project):
        """Return the current existing issues data as list of dicts."""
        data = []

        # Get the data from redcap
        _fields = [self._dfield()]
        rec = self._rc.export_records(
            records=[project],
            forms=['issues'],
            fields=_fields,
        )
        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'issues']

        # Only unresolved issues
        rec = [x for x in rec if str(x['issues_complete']) != '2']

        # Reformat/rename each record
        for r in rec:
            d = {'PROJECT': r[self._dfield()], 'STATUS': 'FAIL'}
            for k, v in self.issues_rename.items():
                d[v] = r.get(k, '')

            data.append(d)

        # Finally, build a dataframe
        return pd.DataFrame(data, columns=self.column_names('issues'))

    def scans(self, projects=None, scantypes=None, modalities='MR'):
        """Query XNAT for all scans and return a dictionary of scan info."""
        # Code to query XNAT for scans
        if not projects:
            projects = self.projects()

        data = self._load_scan_data(projects, scantypes, modalities)

        # Return as dataframe
        return pd.DataFrame(data, columns=self.column_names('scans'))

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

    def stats(self, project, proctypes=None):
        try:
            """Get the stats data from REDCap."""
            statsrc = self._stats_redcap(project)
        except:
            return pd.DataFrame(columns=['stats_assr'])

        rec = statsrc.export_records(forms=['stats'])

        # Filter out FS6 if found
        rec = [x for x in rec if 'FS6_v1' not in x['stats_assr']]

        # Make a dataframe of columns we need
        df = pd.DataFrame(rec, columns=['stats_assr', 'stats_name', 'stats_value'])
        # df = df[['stats_assr', 'stats_name', 'stats_value']]

        print(df[df.duplicated(['stats_assr', 'stats_name'], keep=False)])
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

    def stattypes(self, project):
        """Get list of projects stat types."""
        return self.proctypes(project)

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

    def proctypes(self, project):
        """Get list of project proc types."""
        types = []

        # Get the processing types from scanning forms
        rec = self._rc.export_records(
            forms=['scanning'],
            records=[project],
            export_checkbox_labels=True,
            raw_or_label='label')

        for r in rec:
            for k, v in r.items():
                # Append types for this scanning record
                if v and k.startswith('scanning_proctypes'):
                    types.append(v)

        # Make the lists unique
        types = list(set((types)))

        if not types:
            types = self._default_proctypes()

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
                scans[k]['RESOURCES'] += ',' + r['xnat:imagescandata/file/label']
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

    def _get_result(self, uri):
        """Get result of xnat query."""
        logging.debug(uri)
        json_data = json.loads(self._xnat._exec(uri, 'GET'))
        result = json_data['ResultSet']['Result']
        return result

    def _scan_info(self, record):
        """Get scan info."""
        info = {}

        for k, v in self.scan_rename.items():
            info[v] = record[k]

        # set_modality
        info['MODALITY'] = self.xsi2mod.get(info['XSITYPE'], 'UNK')

        return info

    def _assessor_info(self, record):
        """Get assessor info."""
        info = {}

        for k, v in self.assr_rename.items():
            info[v] = record[k]

        # TODO: Decode inputs into list or keep as string
        # info['INPUTS'] = utils_xnat.decode_inputs(info['INPUTS'])

        # Get the full path
        info['full_path'] = '/projects/{}/subjects/{}/experiments/{}/assessors/{}'.format(
            info['PROJECT'],
            info['SUBJECT'],
            info['SESSION'],
            info['ASSR'])

        # set_modality
        info['MODALITY'] = self.xsi2mod.get(info['XSITYPE'], 'UNK')

        return info

    def _dfield(self):
        """Name of redcap filed that stores project name."""
        return self._rc.def_field

    def progress(self, projects=None):
        """List of progress records."""
        rec = self._rc.export_records(
            forms=['progress'],
            fields=[self._dfield()])

        if projects:
            rec = [x for x in rec if x[self._dfield()] in projects]

        rec = [x for x in rec if x['redcap_repeat_instrument'] == 'progress']
        rec = [x for x in rec if str(x['progress_complete']) == '2']
        return rec

    def processing_protocols(self, project):
        """Return processing protocols."""
        protocols = []

        return protocols

    def update(self, projects=None):
        """Update projects."""
        if not projects:
            projects = self._projects

        logging.info(f'updating projects:{projects}')

        logging.info('updating automations')
        update_automations(self, projects)

        # print('updating issues')
        # update_issues(self, projects)

        # Only run on intersect of specified projects and projects with stats
        # if the list is empty, nothing will run
        logging.info('updating stats')
        update_stats(self, [x for x in projects if x in self.stats_projects()])

        # check that each project has report for current month with PDF and zip
        logging.info('updating progress')
        update_progress(self, projects)

        # TODO: print('updating jobs') # can't do this until queue is in REDCap,
        # for now we'll continue to use run_build.py

    def stats_projects(self):
        """List of projects that have stats, checks for a stats project ID."""
        _fields = [self._dfield(), 'project_stats']
        rec = self._rc.export_records(fields=_fields)
        return [x[self._dfield()] for x in rec if x['project_stats']]

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
            logging.info('locating new record')
            _ids = utils_redcap.match_repeat(
                self._rc,
                project,
                'progress',
                'progress_datetime',
                progress_datetime)
            repeat_id = _ids[-1]

            # Upload output files
            logging.info(f'uploading files to:{repeat_id}')
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

    def get_source_stats(self, project, subject, session, assessor, stats_dir):
        """Download stats files to directory."""
        resource = 'STATS'
        if 'fmriqa_v4' in assessor:
            resource = 'STATSWIDE'

        xnat_resource = self._xnat.select_assessor_resource(
            project,
            subject,
            session,
            assessor,
            resource)

        xnat_resource.get(stats_dir, extract=True)

        return f'{stats_dir}/STATS'

    def set_stats(self, project, subject, session, assessor, stats_data):
        """Upload stats to redcap."""
        if len(stats_data.keys()) > self.max_stats:
            logging.debug('found more than 50 stats:too many, specify subset')
            return

        # Create list of stat records
        rec = [{'stats_name': k, 'stats_value': v} for k, v in stats_data.items()]

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
            #print(rec)
            response = statsrc.import_records(rec)
            assert 'count' in response
            logging.debug('successfully uploaded')
        except AssertionError as err:
            logging.error(f'upload failed:{err}')
        except requests.exceptions.ConnectionError as err:
            logging.error(err)
            logging.info('wait a minute')
            import time
            time.sleep(60)

    def delete_stats(self, project, subject, session, assessor):
        # Get all the repeat instances for this assessor

        # Make the payload
        #payload = {
        #'action': 'delete',
        #'returnFormat': 'json',
        #'content': 'record',
        #'format': 'json',
        #'instrument': 'stats',
        #'token': str(rc.token),
        #'records[0]': str(record_id),
        #'repeat_instance': str(repeat_id),
        #}

        # Call delete
        #result = rc._call_api(payload, 'del_record')

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

        # Determine what scan autos we want to run
        for a in auto_names:
            if rec.get(f'main_etlautos___{a}',  '') == '1':
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
#            logger.error(f'destination project not on XNAT:{dst_project_name}')
#            # TODO: create an issue?
#            return

        # Determine what scan autos we want to run
        for a in auto_names:
            if rec.get(f'main_scanautos___{a}',  '') == '1':
                scan_autos.append(a)

        return scan_autos

    def scanning_protocols(self, project):
        return self._rc.export_records(records=[project], forms=['scanning'])

    def add_issue(
        self,
        description,
        project=None,
        event=None,
        session=None,
        field=None,
        category=None):
        """Add a new issue."""

        # Format for REDCap
        issue_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        record = {
            self._dfield(): project,
            'issue_description': description,
            'issue_date': issue_datetime,
            'issue_subject': subject,
            'issue_session': session,
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
        """Returns list of default processing types"""
        return ['FS7_v1', 'FEOBVQA_v1', 'LST_v1']

    def _default_scantypes(self):
        """Returns list of default scan types"""
        return ['T1', 'CTAC', 'FLAIR']

    def _default_stattypes(self):
        """Returns list of default stats types"""
        return _default_proctypes()

    def primary(self, project):
        """Connect to the primary redcap for this project."""
        project_id = self.project_setting(project, 'primary')
        if not project_id:
            logging.info(f'no primary project id found for project:{project}')
            return None

        return utils_redcap.get_redcap(project_id)

    def xnat(self):
        return self._xnat

    # TODO: def import_stats(self):
    # rather than source_stats from the outside, we call import_stats to tell
    # garjus to go look in xnat (or wherever) to get new stats


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    g = Garjus()
    print(g.projects())
    print(g.scans())
    print(g.assessors())
