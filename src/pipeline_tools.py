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
from db_tools import init_db, run_copy_from


def get_remote_filenames(cfg):
    """ Given configuration, navigate to remote
    FTP site and and fetch a list of zip filenames.
    This function embeds certain key logic assumptions:
    - Only fetch so-called "NAL" files, so filter out any others;
    - Use (configurable) date to fetch only more recent files.
    Args:
        cfg:
            Config objct - contains important info to drive processing.
    Returns:
        List of filenames that can be downloaded.
    """
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
    """ Embed the (albeit brittle) logic to build the canonical
    name of the given zip file on the remote site.
    Args:
        filename:
            filename to format and transform
    Returns:
        Transformed filename.
    """
    return re.sub(r'^([A-Za-z]+) (\d+) Final NAL 2020.zip', 
                    r'\1%20\2%20Final%20NAL%202020.zip', 
                    filename)


def download_files(cfg, filenames):
    """ Download all the files from list of filenames, using
    configurable FTP site.
    Args:
        cfg:
            Config object
        filenames:
            list of filenames to download from remote site.
    Returns:
        download_dir: directory where files have been donwloaded.
    """
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
            logging.critical(f'download failed for {full_download} : {e}')
            continue
        else:
            logging.info(f'downloaded {new_filename}')
    return download_dir


def extract_and_process(cfg, filename):
    """ Do main work of unzipping contents of
    a given zip file, and extracting specified records
    (configurable) which will eventually be inserted into the DB.
    Args:
        cfg:
            Config object
        filename:
            NAme of zip file to extract
    Returns:
        list of records (dicts of data fields)
    """
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
        # TO DO: move this to function/class method which do this in
        # a generator function.
        full_path = download_dir + '/' + zip.namelist()[0]
        with open(full_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            # For each row, iterate over fields, creating the record
            for row in reader:
                cur_record = {}
                for field in fields:
                    field_str = clean_fields(row[field])
                    # bit of biz logic here, as we want to 
                    # treat these fields a bit different - 
                    # again, should move this out.
                    if field == 'CO_NO':
                        cur_record['COUNTY'] = county
                        cur_record[field] = field_str
                    elif field == 'DOR_UC':
                        cur_record[field] = field_str
                        cur_record['DOR_UC_DESC'] = code_dict[field_str]
                    else:
                        cur_record[field] = field_str
                records_list.append(cur_record)

    return records_list


def clean_fields(field_str):
    """ Helper function to strip out any '\' which would
    mess up the copy_from operation. Also strip out whitespace.
    Idea is to use this function to store additional date cleanup
    needed in processing, as we do EDA.
    Args:
        field_str:
            Full field from CSV file to clean.
    Returns:
        cleaned string
    """
    s = field_str.strip()
    s = s.replace('\\', '')
    return s
   

def store_file_recs_json(cfg, filename, file_records):
    """ Store set of file records as JSON.  This function
    is not used, but could be useful (and the format is prettier).
    Args:
        cfg:
            Config object
        filename:
            Name of file to save, based on source
            zip file (preserving county name)
        file_records:
            List of dict objects containing records
    Returns:
        full_path:
            Full path of stored CSV file for later use.
    """
    download_dir = cfg.get('DATA_DOWNLOAD_DIR')
    filename = filename.replace('zip', 'json')
    full_path = download_dir + '/' + filename
    with open(full_path, 'w') as json_file:
        json.dump(file_records, json_file)
    
    return full_path


def store_file_recs_csv(cfg, filename, file_records):
    """ Store a set of file records in a CSV file for
    later use by the Postgres copy_from operation.
    Args:
        cfg:
            Config object
        filename:
            Name of file to save, based on source
            zip file (preserving county name)
        file_records:
            List of dict objects containing records
    Returns:
        full_path:
            Full path of stored CSV file for later use.
    """
    download_dir = cfg.get('DATA_DOWNLOAD_DIR')
    filename = filename.replace('zip', 'csv')
    full_path = download_dir + '/' + filename

    # get headers from first record
    # (Note headers need to be skipped in the copy_from
    # operation, but I prefer to leave them for readability)
    headers = [field for field in file_records[0].keys()]

    with open(full_path, 'w') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter='|')
        csv_writer.writerow(headers)
        for record in file_records:
            values = [val for val in record.values()]
            csv_writer.writerow(values)
    return full_path


def write_to_db(cfg, filename):
    """ Initialize DB, get conn, and invoke
    the copy_from function in db_tools.
    Args:
        cfg:    
            config object
        filename:
            name of file to insert
    Returns:
        True for success; otherwise raise an exception
    """
    try:
        db_conn = init_db(cfg)
        run_copy_from(db_conn, cfg.get("TABLE_NAME"), filename)
    except Exception as e:
        logging.critical(f'failed to insert to db: {e}')
        raise e
    return True



# main to allow us to debug - this logic is now in driver.py
#
if __name__ == "__main__":
    cfg = ConfigMgr(env='DEFAULT', config_src='./config/config.json')

    filenames = get_remote_filenames(cfg)

    download_files(cfg, filenames)

    download_dir = cfg.get('DATA_DOWNLOAD_DIR')
    zip_files = [name for name in os.listdir(download_dir) if name.endswith('.zip')]

  #  zip_files_test = [zip_files[0]]

    for filename in zip_files:
        records_list = extract_and_process(cfg, filename)
        csv_file = store_file_recs_csv(cfg, filename, records_list)
        write_to_db(cfg, csv_file)
    
   