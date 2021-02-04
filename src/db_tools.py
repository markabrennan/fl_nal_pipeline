"""
db_tools.py

Module contains functions to connect to
and insert into Postgres DB

"""

import psycopg2
import sqlalchemy
from sqlalchemy import create_engine


def init_db(cfg):
    connEngine = create_engine("postgresql://eng_test@localhost/eng_test").connect()

def run_copy_from(conn, table, filename):
    cur = conn.cursor()

    with open(filename, 'r') as file_to_write:
        # we need to advance past the headers
        next(file_to_write)
        cur.copy_from(file_to_write, table, sep='|', null='')

    conn.commit()