import logging
import sys
import redcap
from garjus.automations.etl_nihtoolbox_drnair import process


# For testing, we create a connection and run it.
# In production, process_project will be run in garjus.update.automations


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.info('connecting to redcap')
    api_url = 'https://redcap.vanderbilt.edu/api/'
    api_key = sys.argv[1]

    logging.info('Running it')
    results = process(redcap.Project(api_url, api_key))

    logging.info(results)
    logging.info('Done!')
