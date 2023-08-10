import glob
import logging

import redcap

from ..utils_redcap import upload_file


logger = logging.getLogger('garjus.automations.lifedata_file2redcap')


# TODO: get a date from redcap and compare it with data in file

class LifeDataFile2Redcap():
    # map of redcap event to session number in the filename as saved
    event2sess = {
        'baselinemonth_0_arm_2': '1',
        'baselinemonth_0_arm_3': '1',
        'month_8_arm_2': '2',
        'month_8_arm_3': '2',
        'month_16_arm_2': '3',
        'month_16_arm_3': '3',
        'month_24_arm_2': '4',
        'month_24_arm_3': '4',
    }

    # name of field in redcap where file is to be uploaded
    file_field = 'life_file'

    # initialize the uploader with a pycap project and a directory
    def __init__(self, rc, boxdir=''):
        self.rc = rc
        self.boxdir = boxdir
        self.load_secondary_id()

    def load_secondary_id(self):
        # Load secondary ID
        dfield = self.rc.def_field
        sfield = self.rc.export_project_info()['secondary_unique_field']
        rec = self.rc.export_records(fields=[dfield, sfield])
        self.id2subj = {x[dfield]: x[sfield] for x in rec if x[sfield]}

    def upload_life_file(self, record, event, filename):
        '''upload a lifedata file to specified record and event.'''
        upload_file(
            self.rc,
            record,
            self.file_field,
            filename,
            event
        )

    def get_session_files(self, subj, sess_num):
        '''Returns list of file paths for specified subject and session'''
        sess_glob = f'{self.boxdir}/{subj}/LifeData/*{sess_num}*.csv'
        file_list = sorted(glob.glob(sess_glob))
        return file_list

    def process_record(self, r):
        '''Process the given record dictionary.'''
        record_id = r[self.rc.def_field]
        event = r['redcap_event_name']
        sess_num = self.event2sess[event]

        try:
            subj = self.id2subj[record_id]
        except KeyError as err:
            logging.debug(f'record without subject number:{err}')
            return None

        # Check for existing
        if r[self.file_field]:
            logging.debug(f'{subj}:{event}:already uploaded')
            return None

        # Find files for this subject/session
        file_list = self.get_session_files(subj, sess_num)
        file_count = len(file_list)
        if file_count <= 0:
            logging.debug(f'{subj}:{event}:{sess_num}:no files matched')
            return None
        elif file_count > 1:
            logging.error(f'{subj}:{event}:{sess_num}:too many files matched')
            return None

        # Upload the first and only file found
        life_file = file_list[0]
        try:
            logger.debug(f'uploading:{life_file}')
            self.upload_life_file(record_id, event, life_file)
        except(ValueError, redcap.RedcapError) as err:
            logging.error(f'error uploading:{life_file}:{err}')
            return None

        logging.info(f'{subj}:{event}:{self.file_field}:{life_file}:uploaded')

        return {
            'result': 'COMPLETE',
            'type': 'life_file2redcap',
            'subject': self.id2subj[record_id],
            'session': '',
            'scan': '',
            'event': r['redcap_event_name'],
            'field': self.file_field}

    def load_records(self):
        # we want to load id and file from all events with files
        def_field = self.rc.def_field
        fields = [def_field, self.file_field]
        events = self.event2sess.keys()

        # Get records for those fields/events
        logging.info('export records from REDCap')
        records = self.rc.export_records(fields=fields, events=events)

        records = [x for x in records if self.id2subj.get(x[def_field], False)]

        records = sorted(records, key=lambda x: self.id2subj.get(x[def_field]))

        return records

    def run(self):
        results = []

        # Process each record
        for r in self.load_records():
            result = self.process_record(r)
            if result:
                results.append(result)

        return results
