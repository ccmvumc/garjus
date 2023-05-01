"""Processors."""
import os
import logging
import argparse
import json
import yaml
import fnmatch
import re
import itertools
from datetime import date
from uuid import uuid4

from dax.task import NeedInputsException, NoDataException
from dax.task import JOB_PENDING, JOB_RUNNING
from dax.task import NEED_INPUTS, NEED_TO_RUN, NO_DATA, NEEDS_QA, BAD_QA_STATUS
from dax.task import OPEN_STATUS_LIST, RERUN, REPROC, FAILED_NEEDS_REPROC
from dax.processors_v3 import Processor_v3, SgpProcessor, get_resource, get_uri
from dax.errors import AutoProcessorError


def get_scan_status(project_data, scan_path):
    path_parts = scan_path.split('/')
    sess_label = path_parts[6]
    scan_label = path_parts[8]

    _df = project_data.get('scans')
    _df = _df[(_df.SESSION == sess_label) & (_df.SCANID == scan_label)]

    # Check for none found
    if _df.empty:
        #raise XnatUtilsError(f'Invalid scan path:{scan_path}')
        return None

    # Return value from first record
    scan_quality = _df.iloc[0].QUALITY
    return scan_quality


def get_assr_status(project_data, assr_path):
    path_parts = assr_path.split('/')
    sess_label = path_parts[6]
    assr_label = path_parts[8]

    _df = project_data.get('assessors')
    _df = _df[(_df.SESSION == sess_label) & (_df.ASSR == assr_label)]

    # Check for none found
    if _df.empty:
        #raise XnatUtilsError(f'Invalid assessor path:{assr_path}')
        return None

    # Return values from first record
    assr_pstatus = _df.iloc[0].PROCSTATUS
    assr_qstatus = _df.iloc[0].QCSTATUS
    return assr_pstatus, assr_qstatus


def verify_artefact_status(proc_inputs, assr_inputs, project_data):
    # Check artefact status
    logging.debug('checking status of each artefact')
    for artk, artv in list(assr_inputs.items()):
        logging.debug(f'checking status:{artk}')
        inp = proc_inputs[artk]
        art_type = inp['artefact_type']

        if art_type == 'scan' and not inp['needs_qc']:
            # Not checking qc status
            continue

        if art_type == 'scan':
            # Check status of each input scan
            for vinput in artv:
                qstatus = get_scan_status(project_data, vinput)
                if qstatus.lower() == 'unusable':
                    raise NeedInputsException(artk + ': Not Usable')
        else:
            # Check status of each input assr
            for vinput in artv:
                pstatus, qstatus = get_assr_status(project_data, vinput)
                if pstatus in OPEN_STATUS_LIST + [NEED_INPUTS]:
                    raise NeedInputsException(artk + ': Not Ready')

                if qstatus in [JOB_PENDING, REPROC, RERUN]:
                    raise NeedInputsException(artk + ': Not Ready')

                if not inp['needs_qc']:
                    # Not checking qc status
                    continue

                if qstatus in [FAILED_NEEDS_REPROC, NEEDS_QA]:
                    raise NeedInputsException(artk + ': Needs QC')

                for badstatus in BAD_QA_STATUS:
                    if badstatus.lower() in qstatus.split(' ')[0].lower():
                        raise NeedInputsException(artk + ': Bad QC')


def build_task(garjus, assr, info, processor, project_data):
    '''Build a task, create assessor in XNAT, add new record to garjus queue'''
    old_proc_status = info['PROCSTATUS']
    old_qc_status = info['QCSTATUS']
    assr_label = info['ASSR'] 
    job_email = None
    job_email_options = 'FAIL'

    try:
        var2val, inputlist = processor.build_var2val(
            assr,
            info,
            project_data)

        # NOTE:this is where dax would write the slurm file, we are delaying
        # that and instead adding to queue in garjus
        garjus.add_task(
            project_data['name'],
            assr_label,
            inputlist,
            var2val,
            processor.walltime_str,
            processor.memreq_mb,
            processor.yaml_file,
            processor.user_inputs)

        # Set new statuses to be updated
        new_proc_status = JOB_RUNNING
        new_qc_status = JOB_PENDING
    except NeedInputsException as e:
        new_proc_status = NEED_INPUTS
        new_qc_status = e.value
    except NoDataException as e:
        new_proc_status = NO_DATA
        new_qc_status = e.value

    # Update on xnat
    _xsitype = processor.xsitype.lower()
    if new_proc_status != old_proc_status:
        assr.attrs.set(f'{_xsitype}/procstatus', new_proc_status)
    if new_qc_status != old_qc_status:
        assr.attrs.set(f'{_xsitype}/validation/status', new_qc_status)

    # Update local info
    info['PROCSTATUS'] = new_proc_status
    info['QCSTATUS'] = new_qc_status

    return (assr, info)


class Processor_v3_1(Processor_v3):

    def __init__(
        self,
        xnat,
        yaml_file,
        user_inputs=None,
        singularity_imagedir=None,
        job_template='~/job_template.txt',
    ):
        super(Processor_v3_1, self).__init__(
            xnat,
            yaml_file,
            user_inputs=user_inputs,
            singularity_imagedir=singularity_imagedir,
            job_template=job_template)

    def get_assessor(self, session, inputs, project_data):
        proctype = self.get_proctype()
        dfa = project_data['assessors']
        dfa = dfa[(dfa.SESSION == session) & (dfa.PROCTYPE == proctype)]
        dfa = dfa[(dfa.INPUTS == inputs)]

        if len(dfa) > 0:
            # Get the info for the assessor
            info = dfa.to_dict('records')[0]

            logging.debug('matches existing:{}'.format(info['ASSR']))

            # Get the assessor object
            assr = self.xnat.select_assessor(
                info['PROJECT'],
                info['SUBJECT'],
                info['SESSION'],
                info['ASSR'])
        else:
            logging.debug('no existing assessors found, creating a new one')

            # Get the subject for this session
            dfs = project_data['scans']
            subject = list(dfs[dfs.SESSION == session].SUBJECT.unique())[0]
            (assr, info) = self.create_assessor(
                project_data.get('name'),
                subject,
                session,
                inputs)

            logging.debug('created:{}'.format(info['ASSR']))

        return (assr, info)

    def _read_yaml(self, yaml_file):
        """
        Method to read the processor
        :param yaml_file: path to yaml file defining the processor
        """
        logging.debug(f'reading processor from yaml:{yaml_file}')
        with open(yaml_file, "r") as y:
            try:
                doc = yaml.load(y, Loader=yaml.FullLoader)
            except yaml.error.YAMLError as err:
                logging.error(f'failed to load yaml file{yaml_file}:{err}')
                return None

        # NOTE: we are assuming this yaml has already been validated

        # Set version from yaml
        self.procyamlversion = doc.get('procyamlversion')

        # Set requirements from Yaml
        reqs = doc.get('requirements')
        self.walltime_str = reqs.get('walltime', '0-2')
        self.memreq_mb = reqs.get('memory', '16G')
        self.ppn = reqs.get('ppn', 1)
        self.env = reqs.get('env', None)

        # Load the command text
        self.command = doc.get('command')
        self.command_pre = doc.get('pre', None)
        self.command_post = doc.get('post', None)

        # Set Inputs from Yaml
        inputs = doc.get('inputs')

        # Handle vars
        for key, value in inputs.get('vars', {}).items():
            # If value is a key in command
            k_str = '{{{}}}'.format(key)
            if k_str in self.command:
                self.user_overrides[key] = value
            else:
                if isinstance(value, bool) and value is True:
                    self.extra_user_overrides[key] = ''
                elif value and value != 'None':
                    self.extra_user_overrides[key] = value

        # Get xnat inputs, apply edits, then parse
        self.xnat_inputs = inputs.get('xnat')
        self._edit_inputs()
        self._parse_xnat_inputs()

        # Containers
        self.containers = []
        for curc in doc.get('containers'):
            # Set container path
            cpath = curc['path']
            if not os.path.isabs(cpath) and self.singularity_imagedir:
                # Prepend singularity imagedir
                curc['path'] = os.path.join(self.singularity_imagedir, cpath)

            # Add to our containers list
            self.containers.append(curc)

        # Set the primary container path
        container_name = self.command['container']
        for c in self.containers:
            if c.get('name') == container_name:
                self.container_path = c.get('path')

        # Check primary container
        if not self.container_path:
            if len(self.containers) == 1:
                self.container_path = self.containers[0].get('path')
            else:
                msg = 'multiple containers requires a primary to be set'
                logging.error(msg)
                raise AutoProcessorError(msg)

        # Outputs from Yaml
        self._parse_outputs(doc.get('outputs'))

        # Override template
        if doc.get('jobtemplate'):
            _tmp = doc.get('jobtemplate')

            # Make sure we have the full path
            if not os.path.isabs(_tmp):
                # If only filename, we assume it is same folder as default
                print('self.job_template=', self.job_template, _tmp)
                _tmp = os.path.join(os.path.dirname(self.job_template), _tmp)

            # Override it
            self.job_template = os.path.join(_tmp)
        else:
            self.job_template = os.path.join(
                os.path.dirname(self.job_template),
                'job_template_v3.txt')

    def build_var2val(self, assr, info, project_data):
        assr_label = info['ASSR']

        # Make every input a list, so we can iterate later
        inputs = info['INPUTS']
        for k in inputs.keys():
            if not isinstance(inputs[k], list):
                inputs[k] = [inputs[k]]

        # Find values for the xnat inputs
        var2val, input_list = self.find_inputs(assr, inputs, project_data)

        # Append other stuff
        for k, v in self.user_overrides.items():
            var2val[k] = v

        for k, v in self.extra_user_overrides.items():
            var2val[k] = v

        # Include the assessor label
        var2val['assessor'] = assr_label

        # Handle xnat attributes
        for attr_in in self.xnat_attrs:
            _var = attr_in['varname']
            _attr = attr_in['attr']
            _obj = attr_in['object']
            _val = ''

            if _obj == 'subject':
                _val = assr.parent().attrs.get(_attr)
            elif _obj == 'session':
                _val = assr.parent().attrs.get(_attr)
                _ref = attr_in['ref']
                _refval = [a.rsplit('/', 1)[1] for a in inputs[_ref]]
                _val = ','.join([assr.parent().experiment(r).attrs.get(_attr) for r in _refval])
            elif _obj == 'scan':
                _ref = attr_in['ref']
                _refval = [a.rsplit('/', 1)[1] for a in inputs[_ref]]
                _val = ','.join(
                    [assr.parent().scan(r).attrs.get(_attr) for r in _refval]
                )
            elif _obj == 'assessor':
                if 'ref' in attr_in:
                    _ref = attr_in['ref']
                    _refval = [a.rsplit('/', 1)[1] for a in inputs[_ref]]
                    _val = ','.join([assr.parent().assessor(r).attrs.get(_attr) for r in _refval])
                else:
                    _val = assr.attrs.get(_attr)
            else:
                logging.error('invalid YAML')
                err = 'YAML File:contains invalid attribute:{}'
                raise AutoProcessorError(err.format(_attr))

            if _val == '':
                raise NeedInputsException('Missing ' + _attr)
            else:
                var2val[_var] = _val

        return var2val, input_list

    def create_assessor(self, project, subject, session, inputs):
        # returns:
        # : assr pyxnat object
        # : dictionary of assessor info

        xnat_session = self.xnat.select_session(project, subject, session)

        serialized_inputs = json.dumps(inputs)
        guidchars = 8  # how many characters in the guid?
        today = str(date.today())

        # Get a unique ID
        count = 0
        max_count = 100
        while count < max_count:
            count += 1
            guid = str(uuid4())
            assr = xnat_session.assessor(guid)
            if not assr.exists():
                break

        if count == max_count:
            logging.error('failed to find unique ID, cannot create assessor!')
            raise AutoProcessorError()

        # Build the assessor attributes as key/value pairs
        assr_label = '-x-'.join([
            project,
            subject,
            session,
            self.proctype,
            guid[:guidchars]])

        xsitype = self.xsitype.lower()
        kwargs = {
            'label': assr_label,
            'ID': guid,
            f'{xsitype}/proctype': self.proctype,
            f'{xsitype}/procversion': self.procversion,
            f'{xsitype}/procstatus': NEED_INPUTS,
            f'{xsitype}/validation/status': JOB_PENDING,
            f'{xsitype}/date': today,
            f'{xsitype}/inputs': serialized_inputs}

        # Create the assessor
        logging.info(f'creating session asssessor:{assr_label}:{xsitype}')
        assr.create(assessors=xsitype, **kwargs)

        # We keep the inputs as a dictionary in the returned info
        info = {
            'ASSR': assr_label,
            'QCSTATUS': JOB_PENDING,
            'XSITYPE': xsitype,
            'PROCTYPE': self.proctype,
            'PROCVERSION': self.procversion,
            'PROCSTATUS': NEED_INPUTS,
            'INPUTS': inputs}

        return (assr, info)

    def parse_session(self, session, project_data):
        logging.debug(f'parsing session:{session}')
        """
        Parse a session to determine what assessors *should* exist for
        this processor
        """

        artefacts_by_input = self._map_inputs(session, project_data)
        logging.debug('artefacts_by_input=')
        logging.debug(artefacts_by_input)

        parameter_matrix = self._generate_parameter_matrix(artefacts_by_input)
        logging.debug('parameter_matrix=')
        logging.debug(parameter_matrix)

        return parameter_matrix

    def find_inputs(self, assr, inputs, project_data):
        """
        Find the files or directories on xnat for the inputs
        takes an assessor, its input artefacts, its relevant sessions
        and returns the full paths to the input files/directories
        :param assr:
        :param sessions:
        :param assr_inputs:
        :return: variable_set, input_list:
        """
        variable_set = {}
        input_list = []

        # This will raise a NeedInputs exception if any inputs aren't ready
        verify_artefact_status(self.proc_inputs, inputs, project_data)

        logging.debug(self.variables_to_inputs.items())

        # Map from parameters to input resources
        logging.debug('mapping params to artefact resources')
        for k, v in list(self.variables_to_inputs.items()):
            logging.debug('mapping:' + k, v)
            inp = self.proc_inputs[v['input']]
            resource = v['resource']

            logging.debug('vinput={}'.format(v['input']))
            logging.debug(f'resource={resource}')
            logging.debug(inp['resources'])

            # Find the resource
            cur_res = None
            for inp_res in inp['resources']:
                if inp_res['varname'] == k:
                    cur_res = inp_res
                    break

            # TODO: optimize this to get resource list only once
            for vnum, vinput in enumerate(inputs[v['input']]):
                fname = None
                robj = get_resource(assr._intf, vinput, resource)

                # Get list of all files in the resource, relative paths
                file_list = [x._urn for x in robj.files().get('path')]
                if len(file_list) == 0:
                    logging.debug('empty or missing resource')
                    raise NeedInputsException('No Resource')

                if 'fmatch' in cur_res:
                    fmatch = cur_res['fmatch']
                elif cur_res['ftype'] == 'FILE':
                    # Default to all
                    fmatch = '*'
                else:
                    fmatch = None

                if 'filepath' in cur_res:
                    fpath = cur_res['filepath']
                    res_path = resource + '/files/' + fpath

                    # Get base file name to be downloaded
                    fname = os.path.basename(fpath)
                elif fmatch:
                    # Filter list based on regex matching
                    regex = re.compile(fnmatch.translate(fmatch))
                    file_list = [x for x in file_list if regex.match(x)]

                    if len(file_list) == 0:
                        logging.debug('no matching files found on resource')
                        raise NeedInputsException('No Files')

                    if len(file_list) > 1:
                        # Multiple files found, we only support explicit
                        # declaration of fmulti==any1, which tells dax to use
                        # any of the multiple files. We may later support
                        # other options

                        if 'fmulti' in cur_res and cur_res['fmulti'] == 'any1':
                            logging.debug('multiple files, fmulti==any1, using first found')
                        else:
                            logging.debug('multiple files, fmulti not set')
                            raise NeedInputsException('multiple files')

                    # Create the full path to the file on the resource
                    res_path = '{}/files/{}'.format(resource, file_list[0])

                    # Get just the filename for later
                    fname = os.path.basename(file_list[0])
                else:
                    # We want the whole resource
                    res_path = resource + '/files'

                    # Get just the resource name for later
                    fname = resource

                variable_set[k] = get_uri(assr._intf.host, vinput, res_path)

                if 'fdest' not in cur_res:
                    # Use the original file/resource name
                    fdest = fname
                elif len(inputs[v['input']]) > 1:
                    fdest = str(vnum) + cur_res['fdest']
                else:
                    fdest = cur_res['fdest']

                if 'ddest' in cur_res:
                    ddest = cur_res['ddest']
                else:
                    ddest = ''

                # Append to inputs to be downloaded
                input_list.append(
                    {
                        'fdest': fdest,
                        'ftype': cur_res['ftype'],
                        'fpath': variable_set[k],
                        'ddest': ddest,
                    }
                )

                # Replace path with destination path after download
                if 'varname' in cur_res:
                    variable_set[k] = fdest

        logging.debug('finished mapping params to artefact resources')

        return variable_set, input_list

    def _parse_xnat_inputs(self):
        # Get the xnat attributes
        # TODO: validate these
        self.xnat_attrs = self.xnat_inputs.get('attrs', list())

        # Get the xnat edits
        # TODO: validate these
        self.proc_edits = self.xnat_inputs.get('edits', list())

        # get scans
        scans = self.xnat_inputs.get('scans', list())
        for s in scans:
            name = s.get('name')
            self.iteration_sources.add(name)

            types = [_.strip() for _ in s['types'].split(',')]

            resources = s.get('resources', [])

            if 'nifti' in s:
                # Add a NIFTI resource using value as fdest
                resources.append(
                    {'resource': 'NIFTI', 'fdest': s['nifti']})

            if 'edat' in s:
                # Add an EDAT resource using value as fdest
                resources.append(
                    {'resource': 'EDAT', 'fdest': s['edat']})

            needs_qc = s.get('needs_qc', False)

            # Consider an MR scan for an input if it's marked Unusable?
            skip_unusable = s.get('skip_unusable', False)

            # Include 'first' or 'all' matching scans as possible inputs
            keep_multis = s.get('keep_multis', 'all')

            self.proc_inputs[name] = {
                'types': types,
                'artefact_type': 'scan',
                'needs_qc': needs_qc,
                'resources': resources,
                'required': True,
                'skip_unusable': skip_unusable,
                'keep_multis': keep_multis,
            }

        # get assessors
        asrs = self.xnat_inputs.get('assessors', list())
        for a in asrs:
            name = a.get('name')
            self.iteration_sources.add(name)

            try:
                types = [_.strip() for _ in a['types'].split(',')]
            except KeyError:
                types = [_.strip() for _ in a['proctypes'].split(',')]

            resources = a.get('resources', [])
            artefact_required = False
            for r in resources:
                r['required'] = r.get('required', True)
            artefact_required = artefact_required or r['required']

            self.proc_inputs[name] = {
                'types': types,
                'artefact_type': 'assessor',
                'needs_qc': a.get('needs_qc', False),
                'resources': resources,
                'required': artefact_required,
            }

        # Handle petscans section
        petscans = self.xnat_inputs.get('petscans', list())
        for p in petscans:
            name = p.get('name')
            self.iteration_sources.add(name)
            try:
                types = [x.strip() for x in p['types'].split(',')]
            except KeyError:
                types = [x.strip() for x in p['scantypes'].split(',')]

            tracer = [x.strip() for x in p['tracer'].split(',')]

            resources = p.get('resources', [])

            if 'nifti' in p:
                # Add a NIFTI resource using value as fdest
                resources.append(
                    {'resource': 'NIFTI', 'fdest': p['nifti']})

            self.proc_inputs[name] = {
                'types': types,
                'artefact_type': 'scan',
                'needs_qc': p.get('needs_qc', False),
                'require_usable': p.get('require_usable', False),
                'resources': resources,
                'required': True,
                'tracer': tracer,
                'skip_unusable': True,
            }

        if 'filters' in self.xnat_inputs:
            self._parse_filters(self.xnat_inputs.get('filters'))

        self._populate_proc_inputs()
        self._parse_variables()

    def _get_petscans(self, session, project_data):
        petscans = []
        scans = project_data.get('scans')
        subject = ''

        for s in scans.to_dict('records'):
            if s['SESSION'] == session:
                subject = s['SUBJECT']
                break

        if subject:
            petscans = scans[(scans.SUBJECT == subject) & (scans.XSITYPE == 'xnat:petSessionData')].to_dict('records')

        return petscans

    def is_first_mr_session(self, session, project_data):
        is_first = True

        # Get the sessions/dates for this subject
        _dfs = project_data.get('scans')
        subject = _dfs[_dfs.SESSION == session].iloc[0]['SUBJECT']
        logging.debug(f'is_first_mr_session:{session}:{subject}:getting scans')
        scans = _dfs[(_dfs.SUBJECT == subject) & (_dfs.XSITYPE == 'xnat:mrSessionData')]
        scans = scans.sort_values('DATE')

        # Check if this is the first
        if not scans.empty and scans.iloc[0].SESSION != session:
            logging.debug(f'is_first_mr_session:{session}:nope')
            is_first = False

        return is_first

    def _map_inputs(self, session, project_data):
        inputs = self.proc_inputs
        artefacts_by_input = {k: [] for k in inputs}

        # Get lists for scans/assrs for this session
        logging.debug('prepping session data')
        _dfs = project_data.get('scans')
        scans = _dfs[_dfs.SESSION == session].to_dict('records')
        _dfa = project_data.get('assessors')
        assrs = _dfa[_dfa.SESSION == session].to_dict('records')

        petscans = []
        # if this is the first mri, add scans
        if self.is_first_mr_session(session, project_data):
            logging.debug(f'is first mri, adding pets:{session}')
            petscans = self._get_petscans(session, project_data)

        logging.debug('matching artefacts')
        # Find list of scans/assessors that match each specified input
        # for i, iv in list(inputs.items()):
        for i, iv in sorted(inputs.items()):

            if 'tracer' in iv:
                # PET scan
                for p in petscans:
                    # Match the tracer name
                    tracer_name = p['TRACER']
                    tracer_match = False
                    for expression in iv['tracer']:
                        regex = re.compile(fnmatch.translate(expression))
                        if regex.match(tracer_name):
                            # found a match so exit the loop
                            tracer_match = True
                            break

                    if not tracer_match:
                        # None of the expressions matched
                        continue

                    # Now try to match the scan type
                    for expression in iv['types']:
                        regex = re.compile(fnmatch.translate(expression))
                        if regex.match(p['SCANTYPE']):
                            # Found a match, now check quality
                            if p['QUALITY'] == 'unusable':
                                logging.debug('excluding unusable scan')
                            else:
                                artefacts_by_input[i].append(p['full_path'])

            elif iv['artefact_type'] == 'scan':
                # Input is a scan, so we iterate subject scans
                # to look for matches
                for cscan in scans:
                    # First we try to match the session type of the scan
                    # match scan type
                    for expression in iv['types']:
                        regex = re.compile(fnmatch.translate(expression))
                        if regex.match(cscan.get('SCANTYPE')):
                            scanid = cscan.get('SCANID')
                            logging.debug('match found!')
                            if iv['skip_unusable'] and cscan.get('QUALITY') == 'unusable':
                                logging.info(f'Excluding unusable scan:{scanid}')
                            else:
                                # Get scan path, scan ID for each matching scan.
                                # Break if the scan matches so we don't find it again comparing
                                # vs a different requested type
                                artefacts_by_input[i].append(cscan['full_path'])
                                break

            elif iv['artefact_type'] == 'assessor':
                for cassr in assrs:
                    proctype = cassr.get('PROCTYPE')
                    if proctype in iv['types']:
                        # Session type and proc type both match
                        artefacts_by_input[i].append(cassr['full_path'])

        return artefacts_by_input

    def _generate_parameter_matrix(self, artefacts_by_input):
        inputs = self.proc_inputs
        iteration_sources = self.iteration_sources

        # generate n dimensional input matrix based on iteration sources
        all_inputs = []
        input_dimension_map = []

        # check whether all inputs are present
        for i, iv in list(inputs.items()):
            if len(artefacts_by_input[i]) == 0 and iv['required'] is True:
                return []

        for i in iteration_sources:
            # find other inputs that map to this iteration source
            mapped_inputs = [i]
            cur_input_vector = artefacts_by_input[i][:]

            # build up the set of mapped input vectors one by one based on
            # the select mode of the mapped input
            combined_input_vector = [cur_input_vector]

            # 'trim' the input vectors to the number of entries of the
            # shortest vector. We don't actually truncate the datasets but
            # just use the number when transposing, below
            min_entry_count = min((len(e) for e in combined_input_vector))

            # transpose from list of input vectors to input entry lists,
            # one per combination of inputs
            merged_input_vector = [
                [None for col in range(len(combined_input_vector))]
                for row in range(min_entry_count)
            ]
            for row in range(min_entry_count):
                for col in range(len(combined_input_vector)):
                    merged_input_vector[row][col] = combined_input_vector[col][row]

            all_inputs.append(mapped_inputs)
            input_dimension_map.append(merged_input_vector)

        # perform a cartesian product of the dimension map entries to get the
        # final input combinations
        matrix = [
            list(itertools.chain.from_iterable(x))
            for x in itertools.product(*input_dimension_map)
        ]

        matrix_headers = list(itertools.chain.from_iterable(all_inputs))

        # rebuild the matrix to order the inputs consistently
        final_matrix = []
        for r in matrix:
            row = dict()
            for i in range(len(matrix_headers)):
                row[matrix_headers[i]] = r[i]
            final_matrix.append(row)

        return final_matrix

def get_json(xnat, uri):
    return json.loads(xnat._exec(uri, 'GET'))


def get_processor_level(filepath):

    with open(filepath, "r") as y:
        contents = yaml.load(y, Loader=yaml.FullLoader)

    if contents.get('inputs').get('xnat').get('sessions', False):
        return 'subject'
    else:
        return 'session'


def get_processor_procyamlversion(filepath):

    with open(filepath, "r") as y:
        contents = yaml.load(y, Loader=yaml.FullLoader)
        return contents.get('procyamlversion')


def filter_matches(match_input, match_filter):
    return re.match(fnmatch.translate(match_filter), match_input)


def filter_labels(labels, filters):
    filtered_labels = []

    for f in filters:
        filtered_labels += [x for x in labels if filter_matches(x, f)]

    return list(set(filtered_labels))


def load_from_yaml(
    xnat,
    filepath,
    user_inputs=None,
    singularity_imagedir=None,
    job_template=None,
):
    """
    Load processor from yaml
    :param filepath: path to yaml file
    :return: processor
    """

    processor = None
    proc_level = get_processor_level(filepath)

    if proc_level == 'subject':
        logging.debug('loading as SGP:{}'.format(filepath))
        processor = SgpProcessor(
            xnat,
            filepath,
            user_inputs,
            singularity_imagedir,
            job_template)
    else:
        logging.debug('loading as Processor_v3_1:{}'.format(filepath))
        processor = Processor_v3_1(
            xnat,
            filepath,
            user_inputs,
            singularity_imagedir,
            job_template)

    return processor


def build_session_processor(garjus, processor, session, project_data):
    # Get list of inputs sets (not yet matched with existing)
    inputsets = processor.parse_session(session, project_data)

    logging.debug(f'{session}:{processor.name}')

    logging.debug(inputsets)
    for inputs in inputsets:
        if inputs == {}:
            # Blank inputs
            return

        # Get(create) assessor with given inputs and proc type
        (assr, info) = processor.get_assessor(session, inputs, project_data)

        # TODO: apply reproc or rerun if needed

        if info['PROCSTATUS'] in [NEED_TO_RUN, NEED_INPUTS]:
            logging.debug('building task')
            (assr, info) = build_task(
                garjus, assr, info, processor, project_data)

            logging.debug(f'{info}')
            logging.debug('status:{}:{}'.format(info['ASSR'], info['PROCSTATUS']))
        else:
            logging.debug('already built:{}'.format(info['ASSR']))


def build_subject_processor(garjus, processor, subject, project_data):
    logging.debug(f'{subject}:{processor.name}')
    # Get list of inputs sets (not yet matched with existing)
    inputsets = processor.parse_subject(subject, project_data)
    logging.debug(inputsets)

    for inputs in inputsets:

        if inputs == {}:
            # Blank inputs
            return

        # Get(create) assessor with given inputs and proc type

        (assr, info) = processor.get_assessor(
            processor.xnat, subject, inputs, project_data)

        # TODO: apply reproc or rerun if needed

        if info['PROCSTATUS'] in [NEED_TO_RUN, NEED_INPUTS]:
            logging.debug('building task')
            (assr, info) = build_task(
                garjus, assr, info, processor, project_data)

            logging.debug(f'assr after={info}')
        else:
            logging.debug('already built:{}'.format(info['ASSR']))


def build_processor(
    garjus,
    filepath,
    user_inputs,
    project_data,
    include_filters):

    # Get lists of subjects/sessions for filtering
    all_sessions = project_data.get('scans').SESSION.unique()
    all_subjects = project_data.get('scans').SUBJECT.unique()

    # Load the processor
    processor = load_from_yaml(
        garjus.xnat(),
        filepath,
        user_inputs=user_inputs)

    if isinstance(processor, SgpProcessor):
        # Handle subject level processing
        if include_filters:
            include_subjects = filter_labels(all_subjects, include_filters)
        else:
            include_subjects = all_subjects

        # Handle session level processing
        logging.debug(f'include subjects={include_subjects}')

        # Apply the processor to filtered sessions
        for subj in sorted(include_subjects):
            build_subject_processor(garjus, processor, subj, project_data)
    else:
        # Get list of sessions to process
        if include_filters:
            include_sessions = filter_labels(all_sessions, include_filters)
        else:
            include_sessions = all_sessions

        # Handle session level processing
        logging.debug(f'include sessions={include_sessions}')

        # Apply the processor to filtered sessions
        for sess in sorted(include_sessions):
            build_session_processor(garjus, processor, sess, project_data)
