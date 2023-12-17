"""

update will create any missing

"""
import logging
import os


logger = logging.getLogger(__name__)


def update(garjus, projects, scantypes=None):
    """Update project progress."""

    if not scantypes:
        scantypes = garjus.all_scantypes()

    for p in projects:
        logger.debug(f'updating project:{p}')
        update_project(garjus, p, scantypes)


def update_project(garjus, project, scantypes):
    """Update project scans."""

    logger.debug(f'loading existing:{project}')
    try:
        # Get list of scans already done
        _scans = garjus.stats_scans(project, scantypes)
        existing = [(x['scan_session'], x['scan_id']) for x in _scans]
    except Exception as err:
        logger.debug(f'cannot load, check key:{project}:{err}')
        return

    # Get scans for selected project and scantypes
    df = garjus.scans([project], scantypes)
    logger.debug(f'total scans:{len(df)}')

    # Filter to remove already uploaded
    df['SESSION_SCANID'] = list(zip(df.SESSION.astype(str), df.SCANID.astype(str)))
    df = df[~df.SESSION_SCANID.isin(existing)]
    logger.debug(f'scans after filtering out already uploaded:{len(df)}')

    # Filter out unusable
    df = df[df['QUALITY'] != 'unusable']
    logger.debug(f'scans after filtering out unusable:{len(df)}')

    # Iterate xnat scans
    for r in df.sort_values('SESSION_SCANID').to_dict('records'):
        try: 
            if 'JSON' in r['RESOURCES']:
                update_scan(
                    garjus,
                    r['PROJECT'],
                    r['SUBJECT'],
                    r['SESSION'],
                    r['SCANID'],
                )
            else:
                logger.debug(f'no JSON resource')

        except ConnectionError as err:
            logger.info(err)
            logger.info('waiting a minute')
            os.sleep(60)


def update_scan(garjus, proj, subj, sess, scan):
    """Update scan stats."""

    try:
        stats = garjus.get_scan_stats(proj, subj, sess, scan)
    except Exception as err:
        logger.warn(f'could not set stats:{sess}:{scan}:{err}')
        return

    if stats.get('duration', False) or stats.get('tr', False):
        # we go something so upload it
        try:
            logger.debug(f'uploading:{proj}:{subj}:{sess}:{scan}')
            garjus.set_scan_stats(proj, subj, sess, scan, stats)
        except Exception as err:
            logger.warn(f'could not set stats:{sess}:{scan}:{err}')
            return
