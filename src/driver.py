"""
driver.py

Main driver code for running, processing, and testing.
1) Instantiate a ConfigMgr object to fetch config;
2) Get remote filenames from FTP site (configurable);
3) Given list of filenames, download them;
4) For each file, extract and process contents:
    - parse configurable list of fields
5) For each file, store parsed records in CSV file
for later uploading to the DB;
6) Write to the DB - first init the DB, get a conn,
then invoke a copy_from function in the db_tools module.
"""

import sys
import os
import logging
from config_mgr import ConfigMgr
from pipeline_tools import get_remote_filenames, download_files 
from pipeline_tools import extract_and_process, store_file_recs_csv
from pipeline_tools import write_to_db



def main(env, config_src):
    """ Main routine drives the work.
    Args:       
        config_src: location of configuration
    Returns:    Zero for success; 1 for exception (for external callers)
    """
    cfg = ConfigMgr(env, config_src)
    logging.info('Driver: About to ingest and process texts.')

    try:
        filenames = get_remote_filenames(cfg)
        download_files(cfg, filenames)
        download_dir = cfg.get('DATA_DOWNLOAD_DIR')
        zip_files = [name for name in os.listdir(download_dir) if name.endswith('.zip')]

        for filename in zip_files:
            logging.info(f'processing: {filename}')
            records_list = extract_and_process(cfg, filename)
            csv_file = store_file_recs_csv(cfg, filename, records_list)
            write_to_db(cfg, csv_file)
            logging.info(f'wrote {csv_file} to DB')

    except Exception as e:
        logging.critical(f'Process failure - exception:  {e}')
        sys.stderr.write(f'Process failure - exception:  {e}\n')
        sys.stderr.flush()
        exit(1)

    # we've successfully concluded all processing so exit 0
    logging.info(f'completed processing.')
    return 0



if __name__ == "__main__":
    # check if the config label has been passed on the 
    # command line:
    if len(sys.argv) > 1:
        env = sys.argv[1]
    else:
        env = 'DEFAULT'
    sys.stderr.write(f'Config environment: {env}\n')
    sys.stderr.flush()

    ret_val = main(env=env, config_src='config/config.json')

    sys.stderr.write(f'Completed processing.\n')
    sys.stderr.flush()

    sys.exit(ret_val)
