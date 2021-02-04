"""
db_tools.py

Module contains functions to connect to
and insert into Postgres DB

"""

import logging
import sys
from functools import wraps
import psycopg2
from config_mgr import ConfigMgr
import data_model


def db_singleton(init_func):
    """ Singleton function ensures
    we only initialize the DB once,
    and subsequent calls ot init_db
    fetch the single conn instance.
    Args:
        init_func:
            The init_db function to be
            wrapped by the decorator.
    Returns:
        The wrapper function
    """
    conn = None
    @wraps(init_func)
    def wrapper(cfg):
        nonlocal conn
        if conn is None:
            logging.info(f'initializing db now')
            conn = init_func(cfg)
        return conn
    return wrapper


@db_singleton
def init_db(cfg):
    """ Initialize DB connection from config, and
    return a connection object for use.
    Args:
        cfg:   
            Config object which contains DB details
    Returns:
        connection object
    """

    # the connect string differs depending on whether
    # we're connecting to a local DB, or using
    # the DB on the EC2 instance:
    if cfg.env == 'REMOTE':
        template_str = "host={} dbname={} user={}"
        connect_str = template_str.format(cfg.get("DB_HOST"),
                                        cfg.get("DB_NAME"),
                                        cfg.get("DB_USER"))
    else:
        template_str = "host={} dbname={}"
        connect_str = template_str.format(cfg.get("DB_HOST"),
                                        cfg.get("DB_NAME"))
                                    

    logging.info(f'initializing db')
    try:
        conn = psycopg2.connect(connect_str) 
    except Exception as e:
        logging.critical(f'failed to initialize DB: {e}')
        raise e
    else:
        return conn


def run_copy_from(conn, table, filename):
    """ Given a connection object, a table name, and
    a (CSV) filename of records, run the copy_from 
    operation to upload the CSV records to the DB table.
    Args:
        conn:
            DB connection object
        table:
            DB table to which upload/insert
        filename:
            CSV file to upload
    Returns:
        True for success; raise exception otherwise.
    """
    try:
        cur = conn.cursor()

        with open(filename, 'r') as file_to_write:
            # we need to advance past the headers
            next(file_to_write)
            cur.copy_from(file_to_write, table, sep='|', null='')

        conn.commit()
    except Exception as e:
        logging.critical(f'failed to upload to DB - {e}')
        raise e

    return True


if __name__ == "__main__":
    # if we are running as a standalone, then simply
    # create the nal_property_records table.
    cfg = ConfigMgr('config/config.json')

    conn = init_db(cfg)

    create_stmt = data_model.TABLE_CREATE
    cur = conn.cursor()

    try:
        cur.execute(create_stmt)
        conn.commit()
        logging.info(f'created table')
    except Exception as e:
        logging.critical(f'failed to create table! : {e}')
        sys.stderr.write(f'failed to create table! : {e}\n')
        sys.stderr.flush()
        exit(1)
    else:
        sys.stderr.write('created table\n')
        sys.stderr.flush()
        exit(0)


