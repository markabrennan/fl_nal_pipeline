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
import logging
from config_mgr import ConfigMgr


def main(file1, file2, config_src=None):
    """ Main routine drives the work.
    Args:       
        file1 and file2: filename strings
        config_src: location of configuration
    Returns:    Zero for success; -1 for exception (for external callers)
    """
    cfg = ConfigMgr(config_src)
    logging.info('Driver: About to ingest and process texts.')

    try:
        pass


    except Exception as e:
        logging.critical(f'Process failure - exception:  {e}')
        sys.stderr.write(f'Process failure - exception:  {e}\n')
        sys.stderr.flush()
        exit(1)

    # we've successfully concluded all processing so exit 0
    return 0


if __name__ == "__main__":

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    ret_val = main(file1, file2, config_src='config/config.json')

    sys.exit(ret_val)
