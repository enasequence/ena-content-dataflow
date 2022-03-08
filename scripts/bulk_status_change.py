#!/usr/bin/python3

import argparse, cx_Oracle, os, sys
import pandas as pd
from getpass import getpass

__author__ = 'Nadim Rahman, Carla Cummins'
"""
* bulk_status_change.py - README BEFORE RUNNING *

This script has been generated to change the status of data objects in bulk. It can be edited to support running general queries in bulk.
This was required, as SQL has a maximum of 1000 accessions which it can UPDATE at once, so queries/accessions often require splitting, which
this script does.

To run this script, you need to install cx_Oracle (https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html). Then define
the path to the folder with the client_lib_dir variable below. The script uses Python 3+ (https://www.python.org/downloads/) and Python Pandas
(https://pandas.pydata.org/docs/getting_started/install.html).

Ensure that you have provided the host, port and service name for both production (below) and test (at the bottom of the script) databases.

Example usage:
python3 bulk_status_change.py -a <ACCESSIONS_FILE.TXT>
python3 bulk_status_change.py -a <ACCESSIONS_FILE.TXT> -t
python3 bulk_status_change.py -a <ACCESSIONS_FILE.TXT> > Output.txt

ACCESSIONS_FILE.TXT includes INSDC accessions - one per line.
"""

# CONFIGURATION - TO BE COMPLETED #
# Requires replacement of XXX sections - see sql_standard for the appropriate syntax. Note you do not need to provide any IDs within your query.
query = "UPDATE XXX SET XXX, XXX, XXX WHERE XXX in "
db = {'HOST': 'XXX', 'PORT': XXX, 'SERVICE': 'XXX'}

#client_lib_dir = os.getenv('ORACLE_CLIENT_LIB')
client_lib_dir = 'pathto/instantclient_XXX'
if not client_lib_dir or not os.path.isdir(client_lib_dir):
    sys.stderr.write("ERROR: Environment variable $ORACLE_CLIENT_LIB must point at a valid directory\n")
    exit(1)
cx_Oracle.init_oracle_client(lib_dir=client_lib_dir)

###################################

# Potential future implementations:
#   - executemany() may be a better way of running queries here.
#   - Better catching of results once a query has been carried out.
#   - Finding an alternative to cx_Oracle that is easier to manage.


def get_args():
    """
    Handle script arguments
    :return: Script arguments
    """
    parser = argparse.ArgumentParser(prog='main.py', formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog="""
        + =========================================================== +
        |  bulk_status_change.py                                      |
        |  Change the status of data objects via a bulk list of       |
        |  accessions.                                                |
        + =========================================================== +
        """)
    parser.add_argument('-a', '--accessions', help='Location of a file containing a list of accessions')
    parser.add_argument('-t', '--test', help='Specify usage of ERATEST', action='store_true')
    args = parser.parse_args()
    return args


def colfile_to_list(filename):
    """
    Read a file containing a column of IDs from SQL output into a dataframe and then convert to a list
    :param filename: Name of file containing list of SQL IDs in a column
    :param path_prefix: Path to the location of filename
    :return: Dataframe containing the IDs
    """
    cols = pd.read_csv(filename, sep='\t', header=None)[0].tolist()       # Convert a the column of IDs into a list ([0] needed to index the column)
    return(cols)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def print_list(normal_list):
    """
    :param normal_list: A normal Python list
    :return: List items in a SQL command
    """
    sql_list = str(normal_list)[1:-1]       # Add quotes around each item within the list
    return(sql_list)


class MetadataFromDatabase:
    # Class object which handles obtaining metadata from database
    def __init__(self, sql_query, host, port, service):
        self.query = sql_query          # SQL query to obtain metadata
        self.host = host            # Host name for the database connection
        self.port = port            # Port number for the database connection
        self.service = service          # Service name for the database connection


    def get_oracle_usr_pwd(self):
        """
        Obtain credentials to create an SQL connection
        :return: Username and password for a valid SQL database account
        """
        self.usr = input("Username: ")  # Ask for username
        self.pwd = getpass()  # Ask for password and handle appropriately


    def setup_connection(self):
        """
        Set up the database connection
        :return: Database connection object
        """
        self.connection = None
        try:
            dsn = cx_Oracle.makedsn(self.host, self.port,
                                    service_name=self.service)  # Try connection with credentials
            self.connection = cx_Oracle.connect(self.usr, self.pwd, dsn, encoding="UTF-8")
        except cx_Oracle.Error as error:
            print(error)


    def fetch_metadata(self):
        """
        Obtain metadata from ERAPRO database
        :return: Dataframe of metadata
        """
        self.get_oracle_usr_pwd()  # Obtain credentials from script operator
        self.setup_connection()  # Set up the database connection using the credentials
        if self.connection is not None:
            cursor = self.connection.cursor()
            queries = self.query.split("\n")        # Separate queries to run, error produced if run all at once
            all_queries = [string for string in queries if string != ""]        # Remove any empty items in list

            for query in all_queries:
                search_query = cursor.execute(query)  # Query the database with the SQL query
                print('*' * 100)
                print("Ran SQL query:\n{}".format(query))
                print('*' * 100)
            self.connection.commit()
            self.connection.close()


if __name__ == '__main__':
    args = get_args()

    if args.test is True:
        db = {'HOST': 'XXX', 'PORT': XXX, 'SERVICE': 'XXX'}            # Specify test database

    accessions = colfile_to_list(args.accessions)           # Get the list of accessions from file
    batched_accessions = chunks(accessions, 1000)           # Split the list into separate lists of maximum 1,000 accessions

    final_sql = ''
    for batch in batched_accessions:
        sql_string = "("+str(print_list(list(batch)))+")"           # Create the accession string for the SQL query
        sql_query = query + sql_string + "\n"           # Finalise the SQL query with the accession string
        final_sql = final_sql + sql_query
    db_connect = MetadataFromDatabase(final_sql, db['HOST'], db['PORT'], db['SERVICE'])         # Connect to the database and run the query
    db_connect.fetch_metadata()
