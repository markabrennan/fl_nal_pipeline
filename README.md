# fl_nal_pipeline project

#fl_nal_pipeline
## Intro
This project is a front-to-back data pipeline that fetches specific property tax record files from the state of Florida’s department of revenue, ingests and cleans specific fields, prepares output files for bulk data loading, and finally bulk loads data into a table in a Postgres DB.

The requirements for this project lie elsewhere, but this document will address and review some of them, while sharing my approach and areas of focus.

## Running the Code
The code was developed, tested, and run on a local MacOS environment, using a local PostgresSQL DB instance.  It was also run against an EC2 instance provided for the project, using an installed PostgresSQL DB instance.  The code has been run on this instance, populating a single new DB table.

The code can be run through a single script, but for the EC2 instance it’s imperative to pass the “REMOTE” argument. Locally I use “DEFAULT” as a command-line arg or, in its absence, the script defaults to that environment. This allows me to toggle different configuration, one for my local DB, the other for the DB on EC2.

On the very first run, the DB table needs to be created.  In the project root directory - fl_nal_pipeline - run the following:

$ ./init_db.sh REMOTE

Then, run the pipeline process:

$ ./run.sh REMOTE

## Code Flow and Functionality /Approach, Motivation, and Considerations
This project focuses on well-engineered code, possibly at the expense of deriving more meaningful insights and statistics from the data. As a data engineer, my focus is on writing robust and resilient code.  To that end, the code should handle (most) exceptions gracefully, logging errors. In the absence of errors, logging provides meaningful status of the program run, but little in the way of stats (which, however, can be easily added later).

The script is meant to be run from start to finish, as a stand-alone program. It comprises the following steps:
- Hit a configured FTP site to scrape the list of tax revenue file names, using the following criteria:
	- Find files dated later than a configurable date (set to November 9, 2020)
	- Only collect files (or filenames, at this stage) for so-called “NAL” (“Name-Address-Legal”) files.
- Collect relevant filenames
- Pass these filenames one by one to a function that downloads the actual zip file from the FTP site.
- Extract each zip file (each zip file contains a single CSV file) and ingest relevant fields. These fields are configurable, but have been specified per the external requirements.  Some rudimentary cleansing is applied to these fields, with the idea that cleansing will be augmented once more analysis is performed on the data. The extraction function collects a list of Python dicts comprising each file record.
- Each set of file records is subsequently passed to a function to save it to a new CSV file, for later DB loading.
- Each new new CSV file name is then passed to a DB function to perform the Postgres bulk copy (“copy_from”) function, which bulk loads data from a CSV file into a designated table._
This end-to-end pipeline could be decomposed into separate scripts. For example, bulk downloading could be scheduled at one time; another schedule might kick off a job to do the ETL on each downloaded zip file.  And finally, yet another job might run later to bulk upload the “clean” data into the DB. Such jobs might be scheduled and coordinated through a tool like Airflow.  Regardless, the organization of the code should make this decomposition fairly easy.  Most of the main ETL work is done in the pipeline_tools mode, which was designed to be easily called._

A driver module coordinates the end-to-end processing, essentially encapsulating the steps set forth above.

A small DB tools module takes care of initializing a DB connection, and does the work of doing the bulk copy.  This module also has a main() so that it may be invoked as a script in order to run the CREATE table command.  Additionally, the DB init function is decorated with a singleton, to ensure one and only one connection is established and used throughout the script.  This is because the driver program iteratively invokes the bulk copy function on each of the new CSV files, so we would like it to reuse the same DB connection.

Interestingly, this script is functionally composed, with little object-oriented programming, save for the Config Mgr class, which manages the fetching of configuration throughout the program run.  Depending on use cases, some of the ETL “tools” could be composed into classes for better re-use; but such refactoring should be pragmatically driven.

The following functions, while re-useable, also bake in certain assumptions about the data into code logic. Such assumptions could possibly be factored out, for example:
- get_remote_filenames:  this function embeds certain regex patterns used to match files of interest. This should be factored out.
- convert_filename_to_ftp:  this again uses a regex pattern to convert the readable filename on the FTP site into a canonical FTP directory listing - but this is likely too brittle!_
- extract_and_process: this is the main ETL workhorse. While the list of fields is configurable, and may be extended, the function still hard-codes certain logic for certain fields. There is also an implicit dependence on ordering in the dictionary records, which corresponds to ordering in the CSV, which in turn corresponded to the target DB table layout. Thus, there is implicit coupling between the DB and the code, which is bad! This should be factored out.  Additionally, the file processing could be moved out altogether, and wrapped in a generator, so that field processing can be done on-demand. As it is, the function assumes that the entire contents of the extracted zip file should be ingested (which is likely true, but may not always be the case).
- clean_fields:  this function does minimal cleaning, based on bad DB loads with a stray ‘\’ character. It also strips out whitespace from the left and right sides of a field. Other cleaning will likely need to be added, but it can be packaged in this single function._
- store_file_recs_json: before storing to CSV for the bulk copy, I thought it would be useful to store the ETL data in JSON form, which is prettier, but this function is ultimately not used in the script._

### Configuration
A JSON config file externalizes some important configuration information, such as:
- Log file name and level
- DB details
- Download directory name (to be created if it doesn’t exist)
- List of CSV fields of interest for this project
- Name and location of the DOR Use Code mapping file

## Database Table
The single DB table - nal_property_records - comprises a simple mapping of the specified CSV fields of interest and DB columns. Unfortunately, I was not able to implement PostgresSQL’s “SERIAL” auto-incrementing key functionality by way of the psycopg2 module, which means the primary key is the parcel ID, which is a archer - this is not ideal. Future iterations should address and enhance the data model.

## Data Observations
Alas, as suggested above, this project did not arrive at a particularly well-formed and insightful analysis of the data sets. Again, that was because the focus was on engineering good code, which I hope I was able to do.

However, thanks to the mapping of DOR Use Codes to their descriptive equivalents, it is clear that many of the records apply to non-traditional real estate, as it were, including mobile homes, “grazing land”, orchards, vacant residential property, timberland, etc. As such, many of these records lack attributes such as specific addresses, “year built”, etc.  So too, many records lack sales records, but typically have a “Just Value” field, which represents the appraiser’s value of the asset.

## Future Work
Clearly there needs to be a lot of analysis done on the data, including working up joins with the pre-existing Zillow data (available through the same DB instance). While this kind of EDA (exploratory data analysis) should be a normal part of a project like this, due to time constrains I focused almost exclusively on code.

For example, how do the “JV” values compare w/ Zillow numbers?

Also, as already noted, in addition to trying to make some functionality more generic and re-usable (possibly in the form of classes), the script’s functionality (fetching filing names, downloading files, doing ETL, bulk copying into the DB) could also be decomposed into separate, independent scripts. But this should be driven by specific use cases.



