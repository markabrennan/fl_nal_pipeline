"""
pipeline_tools.py

Collect the functions we need to
fetch remote files, unzip them, parse them
and then upload. 

"""
import os
import csv
import json
import logging
import re
from urllib.request import urlopen
from urllib.request import urlretrieve
from zipfile import ZipFile
from contextlib import closing
import datetime
import psycopg2
from config_mgr import ConfigMgr
from db_tools import run_copy_from


def get_remote_filenames(cfg):
    filenames = []
    ftp_site = cfg.get('FTP_SITE')
    #
    # The following static regex patterns drive our parsing.
    # Note these should probably be configurable; also note
    # that the file name pattern embeds # our criteria to match 
    # only NAL files!
    #
    filename_pat = re.compile(r'\d{5,20} ([A-Za-z]+ .*NAL.*zip)')
    # simple data regex pattern
    date_pat = re.compile(r'^\d{1,2}-\d{1,2}-\d{1,2}')
    format_str = '%m-%d-%y'

    # The minimum file date is configurable:
    # we only want file names later than Nov 9, 2020.
    min_dt = datetime.datetime.strptime(cfg.get('MIN_FILE_DATE'), '%m/%d/%y')
    logging.info(f'get_remote_filenames: minimum date:  {min_dt}')
    print(min_dt)

    # open the remote FTP site and grab relevant file names
    logging.info(f'get_remote_filenames: going to hit remote site: {ftp_site}')
    with closing(urlopen(ftp_site)) as ftp_site:
        for file_byte_str in ftp_site:
            # decode string first
            file_str = file_byte_str.decode('utf-8')
            # get date and check it's 
            date_match = date_pat.match(file_str) 
            if date_match:
                date_str = date_match[0]
                date_obj = datetime.datetime.strptime(date_str, format_str)
                if date_obj > min_dt:
                    # get file name string
                    filename = ''
                    file_match = filename_pat.search(file_str)
                    if file_match: 
                        filename = file_match[1]
                        filenames.append(filename)

    return filenames


def convert_filename_to_ftp(filename):
    return re.sub(r'^([A-Za-z]+) (\d+) Final NAL 2020.zip', 
                    r'\1%20\2%20Final%20NAL%202020.zip', 
                    filename)


def download_files(cfg, filenames):
    ftp_site = cfg.get('FTP_SITE')
    download_dir = cfg.get('DATA_DOWNLOAD_DIR')
    # ensure download dir exists
    if not os.path.isdir(download_dir):
        logging.debug("download dir doesn't exist: creating it")
        os.mkdir(download_dir)

    for filename in filenames:
        # get full-qualified file name through conversion
        full_download = ftp_site + '/' + convert_filename_to_ftp(filename)
        try:
            # open remote file
            new_filename = filename.replace(' ', '_')
            full_file_path = download_dir + '/' + new_filename

            urlretrieve(full_download, full_file_path)
        except Exception as e:
            #localfile.close()
            print(f'download failed for {full_download}: {e}')
            logging.critical(f'download failed for {full_download} : {e}')
            continue
        else:
            logging.info(f'downloaded {new_filename}')


def extract_and_process(cfg, filename):
    # get land use codes from config
    dor_code_file = cfg.get('DOR_CODE_FILE')
    if dor_code_file is None:
        raise Exception("Missing DOR Codes!")
    with open(dor_code_file, 'r') as codes:
        code_dict = json.load(codes)

    # get configurable list of fields we want:
    fields = cfg.get('CSV_FIELDS')
    if fields is None:
        raise Exception("Missing CSV fields!")

    # get download dir
    download_dir = cfg.get('DATA_DOWNLOAD_DIR')
    if download_dir is None:
        raise Exception("Download directory doesn't exist!")

    # extract the county name from the file name
    county_pat = re.compile(r'^([A-Za-z]+)_')

    # list of dictionaries containing our records:
    records_list = []

    full_zip_path = download_dir + '/' + filename
    with ZipFile(full_zip_path) as zip:
        m = county_pat.match(filename) 
        county = ''
        # get the match pattern for county
        if m:
            county = m[1]
        # now extract the CSV file
        zip.extractall(path=download_dir)

        # iterate over the CSV file and project out the required fields
        full_path = download_dir + '/' + zip.namelist()[0]
        with open(full_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                cur_record = {}
                for field in fields:
                    if field == 'CO_NO':
                        cur_record['COUNTY'] = county
                        cur_record[field] = row[field]
                    if field == 'DOR_UC':
                        cur_record[field] = row[field]
                        cur_record['DOR_UC_DESC'] = code_dict[row[field]]
                    cur_record[field] = row[field]
                records_list.append(cur_record)

    return records_list


def store_file_recs_json(cfg, filename, file_records):
    download_dir = cfg.get('DATA_DOWNLOAD_DIR')
    filename = filename.replace('zip', 'json')
    full_path = download_dir + '/' + filename
    with open(full_path, 'w') as json_file:
        json.dump(file_records, json_file)


def store_file_recs_csv(cfg, filename, file_records):
    download_dir = cfg.get('DATA_DOWNLOAD_DIR')
    filename = filename.replace('zip', 'csv')
    full_path = download_dir + '/' + filename

    # get headers from first record
    headers = [field for field in file_records[0].keys()]

    with open(full_path, 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter='|')
        csv_writer.writerow(headers)
        for record in file_records:
            values = [val for val in record.values()]
            csv_writer.writerow(values)
    return full_path


def write_to_db(cfg, filename):
    template_str = "host={} dbname={}"
    connect_str = template_str.format(cfg.get("DB_HOST"),
                                      cfg.get("DB_NAME"))

    try:
        conn = psycopg2.connect(connect_str)
        run_copy_from(conn, cfg.get("TABLE_NAME"), filename)
    except Exception as e:
        logging.critical(f'failed to insert to db: {e}')



# main to allow us to debug
#
if __name__ == "__main__":
    cfg = ConfigMgr(env='DEFAULT', config_src='./config/config.json')

    filenames = get_remote_filenames(cfg)

    print(filenames)

    print([filenames[0]])
    download_files(cfg, [filenames[0]])

    download_dir = cfg.get('DATA_DOWNLOAD_DIR')
    zip_files = [name for name in os.listdir(download_dir) if name.endswith('.zip')]
    zip_files_test = [zip_files[0]]

    for filename in zip_files_test:
        records_list = extract_and_process(cfg, filename)
        csv_file = store_file_recs_csv(cfg, filename, records_list)
        write_to_db(cfg, csv_file)
    