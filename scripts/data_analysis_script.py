#!/usr/bin/env python3.8

# Copyright [2020] EMBL-European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse, hashlib, os, subprocess, sys, time
import sys
import shlex
import subprocess
import requests, sys
import re
import requests
import json
from datetime import datetime
import cx_Oracle
from getpass import getpass
import collections
import numpy as np
import pandas as pd
parser = argparse.ArgumentParser(prog='data_analysis_script.py', formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog="""
        + ============================================================ +
        |  European Nucleotide Archive (ENA) data flow monitoring Tool |
        |                                                              |
        |   Tool to to analyse and compare data from NCBI or ENA       |
        + =========================================================== +
This script is used to analyse the data from NCBI and ENA that been produced by data_fetch_script.py.        
        """)
parser.add_argument('-f', '--file', help='path for the input files', type=str, required=True)
args = parser.parse_args()



"""
Request username and password for databases
"""
def get_oracle_usr_pwd():
    if database == 'reads':
        return ['era_reader', 'reader']
    elif database == 'sequences':
        return ['ena_reader', 'reader']



"""
Setup the connection to ENAPRO and ERAPRO. 
"""
def setup_connection():
    oracle_usr, oracle_pwd = get_oracle_usr_pwd()
    client_lib_dir = os.getenv('ORACLE_CLIENT_LIB')
    if database == 'sequences':
        if not client_lib_dir or not os.path.isdir(client_lib_dir):
            sys.stderr.write("ERROR: Environment variable $ORACLE_CLIENT_LIB must point at a valid directory\n")
            exit(1)
        cx_Oracle.init_oracle_client(lib_dir=client_lib_dir)
        connection = None
        try:
            dsn = cx_Oracle.makedsn("ora-ena-pro-hl.ebi.ac.uk", 1531, service_name="ENAPRO")
            connection = cx_Oracle.connect(oracle_usr, oracle_pwd, dsn, encoding="UTF-8")
            return connection
        except cx_Oracle.Error as error:
            print(error)
    else:
        if not client_lib_dir or not os.path.isdir(client_lib_dir):
            sys.stderr.write("ERROR: Environment variable $ORACLE_CLIENT_LIB must point at a valid directory\n")
            exit(1)
        cx_Oracle.init_oracle_client(lib_dir=client_lib_dir)
        connection = None
        try:
            dsn = cx_Oracle.makedsn("ora-era-pro-hl.ebi.ac.uk", 1541, service_name="ERAPRO")
            connection = cx_Oracle.connect(oracle_usr, oracle_pwd, dsn, encoding="UTF-8")
            return connection
        except cx_Oracle.Error as error:
            print(error)


"""
Query ENAPRO dataset, process the data and fetching release date from NCBI nucleotide database. Print to a file.
"""
def fetch_and_filter_seq(connection, output):
   # This Part is for querying ENAPRO
    c = connection.cursor()
    f = open(f"{args.file}/analysis.inENAPRO.sequences.log.txt", "w")
    header = "\t".join(['Accession', 'First_public', 'Last_public', 'status_id' ])
    f.write(str(header) + "\n")
    accession_list_seq = []
    for accession in output:
        c.execute(f"select TRUNC(first_public), TRUNC(last_public), statusid from dbentry where primaryacc# in ('{accession}')")
        for row in c:
            f.write(str(accession) + "\t" + str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + "\n" )
            accession_list_seq.append(accession)
    f.close()

   # Data analysis, to retrive the data that missing from ENAPRO
    print('Data Processing............')
    noENAPRO_f = open(f"{args.file}/analysis.noENAPRO.sequences.log.txt", "w")
    accession_set_seq=set(accession_list_seq)
    accession_set_seq_diff = output.difference(accession_set_seq)
    no_enapro_list= [acc for acc in accession_set_seq_diff ]

   # To fetch the release date from NCBI ( nucleotide database) for the data that missing from ENAPRO ( This command uses 'esearch', 'xtract' and 'efetch' functions, entrez-direct is needed)
    release_date_list = []
    for i in range(0, len(no_enapro_list), 100):
        stripped_list = ', '.join(no_enapro_list[i:i + 100])
        command = 'esearch -db nucleotide -query "{}" |   efetch -format docsum |xtract -pattern DocumentSummary -element Caption, UpdateDate'.format(stripped_list)
        sp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = sp.communicate()
        if "command not found" in err.decode():
            sys.stderr.write(err.decode() + "\n This command uses 'esearch', 'xtract' and 'efetch' functions.\n "
                                           "You might need to download and install 'entrez-direct' to fetch any data from NCBI. "
                                           "\n Please follow the instructions in the link provided below. "
                                           "\n https://www.ncbi.nlm.nih.gov/books/NBK179288/ \n "
                                           "Please note that 'entrez-direct' only runs on Unix and Macintosh environments or under the Cygwin Unix-emulation environment on Windows \n ")
            exit(1)
        else:
            sys.stderr.write(err.decode())
        stdoutOrigin = sys.stdout
        release_date = out.decode().strip("\n").split("\n")
        for obj in release_date:
            release_date_list.append(obj)
        sys.stdout = stdoutOrigin
    noENAPRO_f.write("\n".join(release_date_list) + "\n")
    noENAPRO_f.close()
    # conn.close()



    """
    Query ERAPRO dataset and processing the data. Print to a file.
    """
def fetch_and_filter_reads(connection, output):
    # This Part is for querying ERAPRO
    c = connection.cursor()
    f = open(f"{args.file}/analysis.inERAPRO.reads.log.txt", "w")
    header = "\t".join(['Accession', 'Status_id' 'First_public', 'Last_updated'])
    f.write(str(header) + "\n")
    accession_list_reads = []
    for accession in output:
        c.execute(
            f"select status_id, first_public, last_updated from experiment where experiment_id in ('{accession}')")
        for row in c:
            f.write(str(accession) + "\t" + str(row[0]) + "\t" + str(row[1]) + "\t" + str(row[2]) + "\n")
            accession_list_reads.append(accession)
    f.close()

    # Data analysis, to retrive the data that missing from ERAPRO
    print('Data Processing...........')
    accession_set_reads=set(accession_list_reads)
    accession_set_reads_diff = output.difference(accession_set_reads)
    noERAPRO_df= pd.DataFrame({'accession': list(accession_set_reads_diff)}, columns=['accession'])

    inner_join = pd.merge(noERAPRO_df, sra_df, on='accession', how='inner')
    inner_join.to_csv(f"{args.file}/analysis.noERAPRO.reads.log.txt")


    """
    getting the difference between reads in NCBI and ENA advanced search. Print to a file.
    """
def reads_dataset_difference ():
    output = set(sra_df.accession).difference(set(ena_read_df.accession))
    length_read_set = len(output)

    f = open(f"{args.file}/NCBI_vs_ENA_{database}.log.txt", "w")
    values = "\n".join(map(str, list(output)))
    f.write(values)
    f.close()
    print('Number of data found in NCBI ( SRA database) but missing in ENA advanced search is: ', length_read_set)
    return output


"""
    getting the difference between sequences in NCBI and ENA advanced search. Print to a file.
"""
def sequence_dataset_difference ():
    output = ncbivirus_set.difference(set(ena_seq_df.accession))
    length_seq_set = len(output)
    f = open(f"{args.file}/NCBI_vs_ENA_{database}.log.txt", "w")
    f.write("\n".join(output) + "\n")
    f.close()
    print('Number of data found in NCBI ( NCBIVirus database) but missing in ENA advanced search is: ', length_seq_set)
    return output


"""
    getting the difference between sequences in COVID-19 portal and ENA advanced search. Print to a file.
"""
def covid_advanced_search_difference (ena_dataset, covid_reads_portal_df):
    # Obtain the reads difference between Advanced search and COVID-19 Portal
    covid_portal_output = set(ena_dataset.accession).difference(set(covid_reads_portal_df.accession))
    covid_diff_leng_set = len(covid_portal_output)
    covid_portal_output_df = pd.DataFrame({'accession': list(covid_portal_output)}, columns=['accession'])

    covid_inner_join = pd.merge(covid_portal_output_df, ena_dataset, on='accession', how='inner')
    covid_inner_join.to_csv(f"{args.file}/Covid19Portal.vs.ENA.advanced.{database}.log.txt",sep="\t", index = None)
    print(f"Number of {database} found in ENA advanced search but missing in COVID-19 data portal is: ", covid_diff_leng_set)


    # to create a list of reads duplicates if present in COVID-19 data Portal
    f_duplicates = open(f"{args.file}/Duplicates.Covid19Portal.{database}.log.txt", "w")
    covid_duplicate_list= []
    for item, count in collections.Counter(covid_reads_portal_df).items():
        if count > 1:
            f_duplicates.write(str(item) + "\n")
            covid_duplicate_list.append(item)
    f_duplicates.close()
    length_covid_duplicate = len(covid_duplicate_list)
    print(f"Number of {database} found duplicated in COVID-19 data portal is: ", length_covid_duplicate)


def advanced_search_ebi_search_difference(ebi_df, ena_df):
    # For data found in EBI search but missing in ENA advanced search
    output_1 = set(ebi_df.accession).difference(set(ena_df.accession))
    setLength_1 = len(output_1)
    ebi_output1_df = pd.DataFrame({'accession': list(output_1)}, columns=['accession'])
    ebi1_inner_join = pd.merge(ebi_output1_df, ebi_df, on='accession', how='inner')
    print(f'Number of {database} found in EBI search but missing in ENA advanced search is: ', setLength_1)
    ebi1_inner_join.to_csv(f"{args.file}/EBIsearch_vs_ENAadvanced_{database}.log.txt", sep="\t", index = None)


    # For data found in ENA advanced search but missing in EBI search
    output_2 = set(ena_df.accession).difference(set(ebi_df.accession))
    setLength_2 = len(output_2)
    ebi_output2_df = pd.DataFrame({'accession': list(output_2)}, columns=['accession'])
    ebi2_inner_join = pd.merge(ebi_output2_df, ena_df, on='accession', how='inner')
    print(f'Number of {database} found in ENA advanced search but missing in EBI search is: ', setLength_2)
    ebi2_inner_join.to_csv(f"{args.file}/ENAadvanced_vs_EBIsearch_{database}.log.txt", sep="\t", index = None)



#############
##  MAIN   ##
#############
database = input("please indicate the dataset type, ex: sequences or reads: ").lower()


if database == 'reads':
    #Connecting to ERAPRO
    sys.stderr.write("Connecting to ERAPRO...\n")
    db_conn = setup_connection()

    #Uploading Files from NCBI, ENA, ebisearch
    sra_df =pd.read_csv(f"{args.file}/NCBI.sra.log.txt", sep="\t", header=None, names =['run_id', 'accession', 'release_date'])
    print ('Number of Reads in NCBI (SRA database) is: ', len(sra_df))
    ena_read_df = pd.read_csv(f"{args.file}/ENA.read_experiment.log.txt", sep="\t", header=None, names =['accession', 'date'])
    print('Number of Reads in ENA Advanced Search is: ', len(ena_read_df))
    ebi_read_df= pd.read_csv(f"{args.file}/EBIsearch.sra-experiment-covid19.log.txt", sep="\t", header=None, names =['accession', 'date'])
    print('Number of Reads in EBI Search is: ', len(ebi_read_df))


    #Uploading files from COVID-19 data Portal

    covid_reads_portal_df = pd.read_csv(f"{args.file}/Covid19DataPortal.raw-reads.log.txt", sep="\t", header=None, names=['accession'])
    print('Number of Reads in COVID-19 data portal is: ', len(covid_reads_portal_df))

    #Obtain the difference between the data in NCBI and ENA
    output = reads_dataset_difference()

    #Obtain the reads difference between Advanced search and COVID-19 Portal, and duplicates if present
    covid_advanced_search_difference(ena_read_df, covid_reads_portal_df)

    # Obtain the reads difference between Advanced search and EBI search
    advanced_search_ebi_search_difference(ebi_read_df, ena_read_df)

    #Querying ERAPRO
    sys.stderr.write("Querying ERAPRO ..........\n")
    fetch_and_filter_reads(db_conn, output)

elif database == 'sequences':
    #Connecting to ENAPRO
    sys.stderr.write("Connecting to ENAPRO...\n")
    db_conn = setup_connection()

    # Uploading Files from NCBI and ENA
    ncbivirus_set = set(open(f"{args.file}/NCBI.ncbivirus.log.txt").read().split())
    print('Number of Sequences in NCBI (NCBIVirus database) is: ', len(ncbivirus_set))
    ena_seq_df = pd.read_csv(f"{args.file}/ENA.sequence.log.txt", sep="\t", header=None, names=['accession', 'date'])
    print('Number of Sequences in ENA Advanced Search is: ', len(ena_seq_df))
    ebi_seq_df = pd.read_csv(f"{args.file}/EBIsearch.embl-covid19.log.txt", sep="\t", header=None, names=['accession', 'date'])
    print('Number of Sequences in EBI Search is: ', len(ebi_seq_df))

    # Uploading files from COVID-19 data Portal
    covid_reads_portal_df = pd.read_csv(f"{args.file}/Covid19DataPortal.sequences.log.txt", sep="\t", header=None, names=['accession'])
    print('Number of Sequences in COVID-19 data portal is: ', len(covid_reads_portal_df))

    #Obtain the difference between the data in NCBI and ENA
    output = sequence_dataset_difference()

    #Obtain the sequence difference between Advanced search and COVID-19 Portal, and duplicates if present
    covid_advanced_search_difference(ena_seq_df,covid_reads_portal_df)

    # Obtain the reads difference between Advanced search and EBI search
    advanced_search_ebi_search_difference(ebi_seq_df, ena_seq_df)


    #Querying ENAPRO
    sys.stderr.write("Querying ENAPRO ..........\n")
    fetch_and_filter_seq(db_conn, output)

else:
    sys.stderr.write(f'The dataset type "{database}" does not exist, please check your spelling and try again')

print ("\n **************** END ***************\n")
