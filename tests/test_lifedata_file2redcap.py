import logging
import sys
import redcap
from garjus.automations.lifedata_file2redcap import LifeDataFile2Redcap


# For testing, we create a connection and run it.
# In production, process_project will be run garjus.update.automations


ROOTDIR = '/tmp/EMA_data'


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.info('connecting to redcap')
    api_url = 'https://redcap.vumc.org/api/'
    api_key = sys.argv[1]
    rc = redcap.Project(api_url, api_key)

    logging.info('Running it')
    results = LifeDataFile2Redcap(rc, ROOTDIR).run()

    logging.info(results)
    logging.info('Done!')
