
import logging
import sys
import redcap
from garjus.automations.etl_arcapp.dailyweekly import process_project


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    logging.info('connecting to redcap')
    api_url = 'https://redcap.vanderbilt.edu/api/'
    api_key = sys.argv[1]

    logging.info('Running it')
    project = redcap.Project(api_url, api_key)
    results = process_project(project)
    logging.info(results)
    logging.info('Done!')
