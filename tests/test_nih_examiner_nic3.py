import logging
import sys
import redcap
import tempfile

from garjus.automations import _run_etl_nihexaminer


# For testing, we create a connection and run it.
# In production, will be run by garjus.update.automations


logger = logging.getLogger('test_nih_examiner_nic3')


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.info('connecting to redcap')
    api_url = 'https://redcap.vanderbilt.edu/api/'
    api_key = sys.argv[1]
    rc = redcap.Project(api_url, api_key)

    logging.info('Running it')
    results = _run_etl_nihexaminer(rc)

    logging.info(results)
    logging.info('Done!')
