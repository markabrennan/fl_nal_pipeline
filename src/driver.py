"""
driver.py

Main driver code for running, processing, and testing.
1) Check environment variables
2) Instantiate a ConfigMgr object to fetch config, including:
    - stop words for text processing
    - data file path
3) Ingest text from each file into doc variable
4) Derive core words from each doc
5) Run comparison scoring function on the two sets of core words
    and log results, as well as emit to standard error (console)
"""

import sys
import os
import logging
from config_mgr import ConfigMgr
from pipeline_tools import get_remote_filenames, download_files 
from pipeline_tools import extract_and_process, store_file_recs_csv
from pipeline_tools import write_to_db



def main(config_src=None):
    """ Main routine drives the work.
    Args:       
        config_src: location of configuration
    Returns:    Zero for success; 1 for exception (for external callers)
    """
    cfg = ConfigMgr(config_src)
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
    return 0



if __name__ == "__main__":
    ret_val = main(config_src='config/config.json')

    sys.exit(ret_val)
