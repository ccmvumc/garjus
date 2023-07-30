import logging
import sys
import redcap
from garjus.automations.lifedata_box2redcap import LifeDataBox2Redcap


# For testing, we create a connection and run it.
# In production, process_project will be run garjus.update.automations


BOXDIR = '/Volumes/SharedData/admin-BOX/Box Sync/Rembrandt EMA Output'


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.info('connecting to redcap')
    api_url = 'https://redcap.vanderbilt.edu/api/'
    api_key = sys.argv[1]  # this is a test copy of REM
    rc = redcap.Project(api_url, api_key)

    logging.info('Running it')
    results = LifeDataBox2Redcap(rc, BOXDIR).run()

    logging.info(results)
    logging.info('Done!')
