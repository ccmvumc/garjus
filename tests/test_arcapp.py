import logging
import sys

if __name__ == "__main__":
    import redcap
    from garjus.automations.etl_arcapp import process_project

    api_url = 'https://redcap.vanderbilt.edu/api/'
    api_key = sys.argv[0]  # this is a test copy of REM
    rc = redcap.Project(api_url, api_key)

    logging.basicConfig(
        format='%(asctime)s - %(levelname)s:%(module)s:%(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

    results = process_project(rc)
    print(results)
    print('Done!')
