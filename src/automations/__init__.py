"""

Garjus automations.

Automation names corresond to folder name.

"""
import logging
import importlib
import tempfile

from ..utils_redcap import download_file, field2events


def update(garjus, projects, autos_include=None, autos_exclude=None):
    """Update project progress."""
    for p in projects:
        logging.info(f'updating automations:{p}')
        update_project(garjus, p, autos_include, autos_exclude)


def update_project(garjus, project, autos_include=None, autos_exclude=None):
    """Update automations for project."""
    scan_autos = garjus.scan_automations(project)

    if autos_include:
        # Apply include filter
        scan_autos = [x for x in scan_autos if x in autos_include]

    if autos_exclude:
        # Apply exclude filter
        scan_autos = [x for x in scan_autos if x not in autos_exclude]

    _run_scan_automations(scan_autos, garjus, project)

    etl_autos = garjus.etl_automations(project)

    if autos_include:
        # Apply include filter
        etl_autos = [x for x in etl_autos if x in autos_include]

    if autos_exclude:
        # Apply exclude filter
        etl_autos = [x for x in etl_autos if x not in autos_exclude]

    for a in etl_autos:
        logging.info(f'{project}:running automation:{a}')
        _run_etl_automation(a, garjus, project)


def _parse_scanmap(scanmap):
    """Parse scan map stored as string into map."""
    # Parse multiline string of delimited key value pairs into dictionary
    scanmap = dict(x.strip().split(':') for x in scanmap.split('\n'))

    # Remove extra whitespace from keys and values
    scanmap = {k.strip(): v.strip() for k, v in scanmap.items()}

    return scanmap


def _run_etl_automation(automation, garjus, project):
    """Load the project primary redcap."""
    results = []

    project_redcap = garjus.primary(project)
    if not project_redcap:
        logging.info('not found')
        return

    if automation == 'etl_nihexaminer':
        results = _run_etl_nihexaminer(project_redcap)
    else:
        # load the automation
        try:
            m = importlib.import_module(f'src.automations.{automation}')
        except ModuleNotFoundError as err:
            logging.error(f'error loading module:{automation}:{err}')
            return

        # Run it
        try:
            results = m.process_project(project_redcap)
        except Exception as err:
            logging.error(f'{project}:{automation}:failed to run:{err}')
            return

    # Upload results to garjus
    for r in results:
        r.update({'project': project, 'category': automation})
        r.update({'description': r.get('description', automation)})
        garjus.add_activity(**r)


def _run_etl_nihexaminer(project):
    """Process examiner files from REDCap and upload results."""
    data = {}
    results = []
    events = []
    fields = []
    records = []
    flank_field = 'flanker_file'
    nback_field = 'nback_upload'
    shift_field = 'set_shifting_file'
    cpt_field = 'cpt_upload'
    done_field = 'flanker_score'

    # load the automation
    try:
        examiner = importlib.import_module(f'src.automations.etl_nihexaminer')
    except ModuleNotFoundError as err:
        logging.error(f'error loading module:examiner:{err}')
        return

    if 'flanker_summfile' in project.field_names:
        # Alternate file field names
        flank_field = ''
        nback_field = 'nback_summfile'
        shift_field = 'set_shifting_summfile'
        cpt_field = 'cpt_summfile'

    # Get the fields
    fields = [
        project.def_field,
        done_field,
        cpt_field,
        'dot_count_tot',
        'anti_trial_1',
        'anti_trial_2',
        'correct_f',
        'correct_l',
        'correct_animal',
        'correct_veg',
        'repetition_f',
        'rule_vio_f',
        'repetition_l',
        'rule_vio_l',
        'repetition_animal',
        'rule_vio_animal',
        'repetition_veg',
        'rule_vio_veg',
        'brs_1',
        'brs_2',
        'brs_3',
        'brs_4',
        'brs_5',
        'brs_6',
        'brs_7',
        'brs_8',
        'brs_9',
    ]

    if 'correct_s' in project.field_names:
        fields = fields.extend([
            'correct_s', 'rule_vio_s, repetition_s',
            'correct_t', 'rule_vio_t, repetition_t',
            'correct_fruit', 'rule_vio_fruit, repetition_fruit',
            'correct_r', 'rule_vio_r, repetition_r',
            'correct_m', 'rule_vio_m, repetition_m',
            'correct_cloth', 'rule_vio_cloth, repetition_cloth',
        ])

    # Determine events
    events = field2events(project, cpt_field)

    # Get records for those events and fields
    records = project.export_records(fields=fields, events=events)

    for r in records:
        data = {}
        record_id = r[project.def_field]
        event_id = r['redcap_event_name']

        if r[done_field]:
            logging.debug(f'already ETL:{record_id}:{event_id}')
            continue

        if not r[cpt_field]:
            logging.debug(f'no data file:{record_id}:{event_id}')
            continue

        logging.info(f'running nihexaminer ETL:{record_id}:{event_id}')

        # Get values needed for scoring
        manual_values = {
            'dot_total': int(r['dot_count_tot']),
            'anti_trial_1': int(r['anti_trial_1']),
            'anti_trial_2': int(r['anti_trial_2']),
            'cf1_corr': int(r['correct_animal']),
            'cf1_rep': int(r['repetition_animal']),
            'cf1_rv': int(r['rule_vio_animal']),
            'brs_1': int(r['brs_1']),
            'brs_2': int(r['brs_2']),
            'brs_3': int(r['brs_3']),
            'brs_4': int(r['brs_4']),
            'brs_5': int(r['brs_5']),
            'brs_6': int(r['brs_6']),
            'brs_7': int(r['brs_7']),
            'brs_8': int(r['brs_8']),
            'brs_9': int(r['brs_9']),
        }

        if r['correct_f']:
            # examiner version 0
            manual_values.update({
                'vf1_corr': int(r['correct_f']),
                'vf1_rep': int(r['repetition_f']),
                'vf1_rv': int(r['rule_vio_f']),
                'vf2_corr': int(r['correct_l']),
                'vf2_rep': int(r['repetition_l']),
                'vf2_rv': int(r['rule_vio_l']),
                'cf2_corr': int(r['correct_veg']),
                'cf2_rep': int(r['repetition_veg']),
                'cf2_rv': int(r['rule_vio_veg'])
            })
        elif r['correct_t']:
            # examiner version 1
            manual_values.update({
                'vf1_corr': int(r['correct_t']),
                'vf1_rep': int(r['repetition_t']),
                'vf1_rv': int(r['rule_vio_t']),
                'vf2_corr': int(r['correct_s']),
                'vf2_rep': int(r['repetition_s']),
                'vf2_rv': int(r['rule_vio_s']),
                'cf2_corr': int(r['correct_fruit']),
                'cf2_rep': int(r['repetition_fruit']),
                'cf2_rv': int(r['rule_vio_fruit'])
            })
        else:
            # examiner version 2
            manual_values.update({
                'vf1_corr': int(r['correct_r']),
                'vf1_rep': int(r['repetition_r']),
                'vf1_rv': int(r['rule_vio_r']),
                'vf2_corr': int(r['correct_m']),
                'vf2_rep': int(r['repetition_m']),
                'vf2_rv': int(r['rule_vio_m']),
                'cf2_corr': int(r['correct_cloth']),
                'cf2_rep': int(r['repetition_cloth']),
                'cf2_rv': int(r['rule_vio_cloth'])
            })

        with tempfile.TemporaryDirectory() as tmpdir:
            # Get files needed
            flank_file = f'{tmpdir}/flanker.csv'
            cpt_file = f'{tmpdir}/cpt.csv'
            nback_file = f'{tmpdir}/nback.csv'
            shift_file = f'{tmpdir}/shift.csv'

            try:
                # Download files from redcap
                logging.debug(f'download files:{record_id}:{event_id}:{flank_file}')
                download_file(project, record_id, event_id, flank_field, flank_file)
                logging.debug(f'download NBack:{record_id}:{event_id}:{nback_field}')
                download_file(project, record_id, event_id, nback_field, nback_file)
                logging.debug(f'download Shift:{record_id}:{event_id}:{shift_field}')
                download_file(project, record_id, event_id, shift_field, shift_file)
                logging.debug(f'download CPT:{record_id}:{event_id}:{cpt_field}')
                download_file(project, record_id, event_id, cpt_field, cpt_file)
            except Exception as err:
                logging.error(f'downloading files:{record_id}:{event_id}')
                continue

            try:
                # Process inputs
                data = examiner.process(
                    manual_values,
                    flank_file,
                    cpt_file,
                    nback_file,
                    shift_file)
            except Exception as err:
                logging.error(f'processing examiner:{record_id}:{event_id}')
                continue

        # Load data back to redcap
        _load(project, record_id, event_id, data)
        results.append({'subject': record_id, 'event': event_id})

    return results


def _run_scan_automations(automations, garjus, project):
    results = []
    proj_scanmap = garjus.project_setting(project, 'scanmap')
    sess_replace = garjus.project_setting(project, 'relabelreplace')
    scan_data = garjus.scanning_protocols(project)
    site_data = garjus.sites(project)
    protocols = garjus.scanning_protocols(project)

    # Build the session relabling
    sess_relabel = _session_relabels(scan_data, site_data)

    # Parse scan map
    if proj_scanmap:
        proj_scanmap = _parse_scanmap(proj_scanmap)

    # Load the project primary redcap
    project_redcap = garjus.primary(project)
    if not project_redcap:
        logging.info('primary redcap not found, cannot run automations')
        return

    # load the automations
    try:
        xnat_auto_archive = importlib.import_module(f'src.automations.xnat_auto_archive')
        xnat_relabel_sessions = importlib.import_module(f'src.automations.xnat_relabel_sessions')
        xnat_relabel_scans = importlib.import_module(f'src.automations.xnat_relabel_scans')
    except ModuleNotFoundError as err:
        logging.error(f'error loading scan automations:{err}')
        return

    # Get xnat connection
    xnat = garjus.xnat()

    # Apply autos to each scanning protocol
    for p in protocols:
        date_field = p['scanning_datefield']
        sess_field = p['scanning_srcsessfield']
        sess_suffix = p['scanning_xnatsuffix']
        src_project = p['scanning_srcproject']

        # Get events list
        events = None
        if p.get('scanning_events', False):
            events = [x.strip() for x in p['scanning_events'].split(',')]

        # Make the scan table that links what's entered at the scanner with
        # what we want to label the scans
        scan_table = _make_scan_table(
            project_redcap,
            events,
            date_field,
            sess_field,
            sess_suffix)

        # Run
        if 'xnat_auto_archive' in automations:
            results += xnat_auto_archive.process_project(
                garjus, scan_table, src_project, project)

    # Apply relabeling
    if 'xnat_relabel_sessions' in automations:
        results += xnat_relabel_sessions.process_project(
            xnat, project, sess_relabel, sess_replace)
    if 'xnat_relabel_scans' in automations:
        results += xnat_relabel_scans.process_project(
            xnat, project, proj_scanmap)

    # Upload results to garjus
    for r in results:
        r['project'] = project
        garjus.add_activity(**r)


def _make_scan_table(
    project,
    events,
    date_field,
    sess_field,
    scan_suffix):
    """Make the scan table, linking source to destination subject/session."""
    data = []
    id2subj = {}

    # Shortcut
    def_field = project.def_field

    # Handle secondary ID
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

    # Get mri records from redcap
    fields = [date_field, sess_field, def_field]
    try:
        rec = project.export_records(fields=fields, events=events)
    except Exception as err:
        logging.error(err)
        return []

    # Only if date is entered
    rec = [x for x in rec if x[date_field]]

    # Only if entered
    rec = [x for x in rec if x[sess_field]]

    # Set the subject and session
    for r in rec:
        d = {}
        d['src_session'] = r[sess_field]
        d['src_subject'] = d['src_session']
        d['dst_subject'] = id2subj.get(r[def_field], '')
        d['dst_session'] = d['dst_subject'] + scan_suffix
        data.append(d)

    return data


def _session_relabels(scan_data, site_data):
    """Build session relabels."""
    relabels = []

    # Build the session relabeling from scan_autos and sites
    for rec in scan_data:
        relabels.append((
            'session_label',
            '*' + rec['scanning_xnatsuffix'],
            'session_type',
            rec['scanning_xnattype']))

    for rec in site_data:
        relabels.append((
            'session_label',
            rec['site_sessmatch'],
            'site',
            rec['site_shortname']))

    return relabels


def _load(project, record_id, event_id, data):
    data[project.def_field] = record_id
    data['redcap_event_name'] = event_id
    data = {k: str(v) for k,v in data.items()}

    try:
        response = project.import_records([data])
        assert 'count' in response
        logging.info(f'uploaded:{record_id}:{event_id}')
    except AssertionError as e:
        logging.error('error uploading', record_id, e)
