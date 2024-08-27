#!/usr/bin/python3

# bulk_datahub_user.py

__author__ = 'Nadim Rahman'

import argparse, json, oracledb, os, sys, yaml
import pandas as pd
from getpass import getpass

# This script generates a query (example below) to add users with appropriate roles to a Data Hub
# begin
#     era.portal_dcc_pkg.add_dcc_user('dcc_test', p_contact_name => 'John Doe', p_email_address => 'jdoe@test.com', p_address => 'Test Avenue', p_submission_account_id => 'Webin-1234', p_role => 'PROVIDER');
#     era.portal_dcc_pkg.add_dcc_user('dcc_test', p_contact_name => 'Jane Doe', p_email_address => 'jdoe@test.com', p_address => 'Test Street', p_role => 'CONSUMER');
# end;
# /

# REQUIREMENTS TO RUN SCRIPT:
#   1) You need an OPS user for ERA databases.
#   2) Add your variables for the database connection in a file called 'config.yaml'. Must include the host, port number and service name for both test and production databases.
#   3) An appropriate spreadsheet (template attached)

# After running your script, check the database table to see if changes have occurred. Run the following SQL query:
#   select * from dcc_user where meta_key='[DATA HUB]';


def get_args():
    '''
    Define and obtain script arguments
    :return: Arguments object
    '''
    parser = argparse.ArgumentParser(prog='analysis_submission.py', formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog="""
        + ======================================================================= +
        |  European Nucleotide Archive (ENA) Bulk Data Hub User Addition/Removal  |
        |                                                                         |
        |  Tool to assign users of a Data Hub in a bulk manner                    |
        + ======================================================================= +
        """)
    parser.add_argument('-m', '--meta_key', help='The name of the Data Hub (e.g. dcc_XXXX)', type=str, required=True)
    parser.add_argument('-u', '--users', help='Path to spreadsheet of user assignment information for the Data Hub', type=str, required=True)
    parser.add_argument('-a', '--action', help='Action to be committed. Options: ADD, REMOVE', choices=['ADD', 'REMOVE'], default='ADD', required=False)
    parser.add_argument('-s', '--submit', help='Submit the query and action before enacting on the database', action='store_true')
    parser.add_argument('-t', '--test', help='Specify whether to use test servers', action='store_true')
    args = parser.parse_args()
    return args


def read_config():
    """
    Read in the configuration file
    :return: A dictionary referring to tool configuration
    """
    with open("config.yaml") as f:
        configuration = yaml.safe_load(f)
    return configuration

class ReadValidateInput:
    # Class object which handles reading in and validating input spreadsheet of user information
    def __init__(self, spreadsheet):
        self.spreadsheet = spreadsheet  # Spreadsheet of user information


    def check_providers(self, providers):
        """
        Check that Webin account IDs have been provided for data providers
        :return:
        """
        empty_columns = providers['submission_account_id'].isnull().any()
        if empty_columns == True:
            sys.exit('> [ERROR]: Please check spreadsheet, all data providers must have a Webin submission account associated to them.')
        else:
            print('> All data providers have Webin submission accounts.')


    def read_and_validate(self):
        """
        Coordinate the read in and validation of the spreadsheet
        :return: Validation related checks and output
        """
        user_info = pd.read_csv(self.spreadsheet, sep='\t', index_col=False, header=0)
        providers = user_info.loc[user_info['role'] == 'PROVIDER']
        self.check_providers(providers)
        return user_info


def construct_query(spreadsheet):
    """
    Construct the SQL query
    :return: An appropriate SQL string to directly add users to the database
    """
    sql_string = "begin\n"
    for index, row in spreadsheet.iterrows():
        if row['role'] == 'CONSUMER':
            consumer_string = "era.portal_dcc_pkg.add_dcc_user('"+(row["datahub"])+"', p_contact_name => '"+(row["fullname"])+"', p_email_address => '"+(row["email"])+"', p_address => '"+(row["address"])+"', p_role => '"+(row["role"])+"');\n"
            sql_string += consumer_string
        elif row['role'] == 'PROVIDER':
            provider_string = "era.portal_dcc_pkg.add_dcc_user('"+(row["datahub"])+"', p_contact_name => '"+(row["fullname"])+"', p_email_address => '"+(row["email"])+"', p_address => '"+(row["address"])+"', p_submission_account_id => '"+(row["submission_account_id"])+"', p_role => '"+(row["role"])+"');\n"
            sql_string += provider_string
    sql_string += "end;"
    return sql_string


class ConnectToDatabase:
    # Class object which handles obtaining metadata from ERAPRO database
    def __init__(self, sql_query, host, port, service_name):
        self.query = sql_query  # SQL query to obtain metadata
        self.host = host
        self.port = port
        self.service_name = service_name

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
            self.connection = oracledb.connect(user=self.usr, password=self.pwd, host=self.host, port=self.port, service_name=self.service_name)
        except oracledb.Error as error:
            print(error)

    def database_action(self):
        """
        Obtain metadata from ERAPRO database
        :return: Dataframe of metadata
        """
        self.get_oracle_usr_pwd()  # Obtain credentials from script operator
        self.setup_connection()  # Set up the database connection using the credentials
        if self.connection is not None:
            cursor = self.connection.cursor()
            search_query = cursor.execute(self.query)  # Query the database with the SQL query
            try:
                df = pd.DataFrame(search_query.fetchall())  # Fetch all results and save to dataframe
            except AttributeError:
                print('Ran query...')
                df = pd.DataFrame()
            self.connection.commit()
            self.connection.close()
            return df


if __name__ == '__main__':
    args = get_args()       # Get script arguments
    configuration = read_config()       # Get configuration variables

    # Handle test database connection
    if args.test == True:
        host = configuration["TEST_HOST"]
        port = configuration["TEST_PORT"]
        service_name = configuration["TEST_SERVICE_NAME"]
    else:
        host = configuration["HOST"]
        port = configuration["PORT"]
        service_name = configuration["SERVICE_NAME"]

    # Obtain file information
    readvalidate_obj = ReadValidateInput(args.users)  # Instantiate object for reading and validating users spreadsheet
    user_info = readvalidate_obj.read_and_validate()  # Obtain cleaned version of spreadsheet in dataframe

    # Construct SQL query
    sql_query = construct_query(user_info)
    with open("output.txt", "w") as text_file:
        text_file.write(sql_query)
    print("""
==========================================
 Query to be ran:
 """+(sql_query)+"""
==========================================
    """)

    if args.submit == True:
        # Connect to the database and implement query only when not in validate mode
        db_conn_obj = ConnectToDatabase(sql_query, host, port, service_name)
        results = db_conn_obj.database_action()
