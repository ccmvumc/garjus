"""Creates html export of data with web app"""
import logging
from datetime import datetime


logger = logging.getLogger('garjus')


def export_html(garjus, filename, projects):
    # Save to file
    print(f'saving html:{filename}:{projects=}')
    logger.debug(f'saving to file:{filename}')
