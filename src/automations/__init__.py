"""

Garjus automations.

Automation names corresond to function name

"""
import logging


def update(garjus, projects, autos_include=None, autos_exclude=None):
    """Update project progress."""
    for p in projects:
        update_project(garjus, p, autos_include, autos_exclude)


def update_project(garjus, project, autos_include=None, autos_exclude=None):
    """Update automations for project."""
    results = []
    # event2sess = {}
    # event2mr = {}
    proj_scanmap = garjus.project_setting(project, 'scanmap')
    sess_replace = garjus.project_setting(project, 'relabelreplace')

    if proj_scanmap:
        proj_scanmap = parse_scanmap(proj_scanmap)

    # Get filtered list for this project
    scan_autos = garjus.scan_automations(project)
    if autos_include:
        # Apply include filter
        scan_autos = [x for x in scan_autos if x in autos_include]

    if autos_exclude:
        # Apply exclude filter
        scan_autos = [x for x in scan_autos if x not in autos_exclude]

    if 'xnat_auto_archive' in scan_autos:
        logging.info(f'{project}:xnat_auto_archive:TBD')

    # Run project-wide relabels
    if 'xnat_relabel_scans' in scan_autos:
        logging.info(f'{project}:xnat_relabel_scans:TBD')


    if 'xnat_relabel_sessions' in scan_autos:
        logging.info(f'{project}:xnat_relabel_sessions:TBD')

    return results


def parse_scanmap(scanmap):
    """Parses scan map stored as string into map"""
    # Parse multiline string of delimited key value pairs into dictionary
    scanmap = dict(x.strip().split(':') for x in scanmap.split('\n'))

    # Remove extra whitespace from keys and values
    scanmap = {k.strip(): v.strip() for k, v in scanmap.items()}

    return scanmap
