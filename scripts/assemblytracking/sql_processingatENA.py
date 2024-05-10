#!/usr/bin/python3

# Copyright [2024] EMBL-European Bioinformatics Institute
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

import os
import pandas as pd
import configparser
from sqlalchemy import create_engine
import logging

def connect_to_database(database):
    # creates the 'engine' object which makes it possible to read an oracle database
    # inputs database info
    print(f'getting {database} login info and creating database connection engine')
    config = configparser.ConfigParser()
    config.read(config_file_path)
    if database == 'ERA':
        username = config['ERAREAD_DETAILS']['userName']
        password = config['ERAREAD_DETAILS']['password']
        host = config['ERAREAD_DETAILS']['host']
        port = config['ERAREAD_DETAILS']['port']
        serviceName = config['ERAREAD_DETAILS']['serviceName']
        engine_string = f"""oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={serviceName}"""
    elif database == 'ENA':
        username = config['ENAREAD_DETAILS']['userName']
        password = config['ENAREAD_DETAILS']['password']
        host = config['ENAREAD_DETAILS']['host']
        port = config['ENAREAD_DETAILS']['port']
        serviceName = config['ENAREAD_DETAILS']['serviceName']
        engine_string = f"""oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={serviceName}"""
    else:
        return 'no valid database selected, choose <ERA> or <ENA>'
    global engine
    logging.basicConfig()
    logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
    engine = create_engine(engine_string)

def check_ERA_for_submission():
    print(f'checking ERA database for new {project} assembly submissions')
    processing_at_ENA = pd.DataFrame(columns=['name', 'submission date', 'project', 'analysis ID', 'status ID', 'process', 'status'])

    global procesing_at_ENA_name_list
    global procesing_at_ENA_haplo_list
    procesing_at_ENA_name_list = []
    procesing_at_ENA_haplo_list = []
    with engine.connect() as conn: # opens the connection to database
        for name in datainput['name to track']: # goes through all the new assemblies added to the tracking file
            print(f'trying {name}')
            # get the primary assembly name to search
            eraname = 'webin-genome-' + name
            # look  up the main assembly
            query_string = f"""select b.analysis_alias, b.first_created, b.bioproject_id, b.analysis_id, b.status_id, a.pipeline_name, a.state
                                        from pipelite2_process a
                                        join analysis b on (process_id = analysis_id)
                                        where analysis_alias in ('{eraname}')"""
            tempdf = pd.read_sql(query_string, conn, parse_dates={'first_created': '%Y-%m-%d %H:%M:%S'})

            # get the haplotype assembly name to search
            eranamehaplo = 'webin-genome-' + name + '_alternate_haplotype'
            #look up the halpotype
            query_string_haplo = f"""select b.analysis_alias, b.first_created, b.bioproject_id, b.analysis_id, b.status_id, a.pipeline_name, a.state
                                            from pipelite2_process a
                                            join analysis b on (process_id = analysis_id)
                                            where analysis_alias in ('{eranamehaplo}')"""
            tempdfh = pd.read_sql(query_string_haplo, conn, parse_dates={'first_created': '%Y-%m-%d %H:%M:%S'})

            outtempdf = None

            if tempdf.empty and tempdfh.empty: # if no assemblies found
                print(name, 'no submission found')
                row_index = datainput.loc[datainput["name to track"] == name].index[0]
                datainput.loc[row_index, "notes"] = 'no submission found'
            elif (len(tempdf) == 2 and tempdfh.empty):
                print(name, ' - added to Processing at ENA without haplotype')
                # if the assembly is found + COMPLETED, but no haplotype - add assembly only and report it
                row_index = datainput.loc[datainput["name to track"] == name].index[0]
                datainput.loc[row_index, "notes"] = 'no error in processing, added to Processing at ENA without haplotype'
                outtempdf = tempdf
                procesing_at_ENA_name_list.append(name)
            elif tempdf.empty and len(tempdfh) == 2:
                print(name, 'only the haplotype found, added to Processing at ENA')
                # if only haplotype found + has no errors, add haplotype only and report it
                row_index = datainput.loc[datainput["name to track"] == name].index[0]
                datainput.loc[row_index, "notes"] = 'only the haplotype found, added to Processing at ENA'
                outtempdf = tempdfh
                procesing_at_ENA_haplo_list.append(name)
            elif (len(tempdf) == 2 and len(tempdfh) == 2):
                print(name, 'assembly and haplotype found, added to Processing at ENA')
                # if both assembly and haplotype found
                row_index = datainput.loc[datainput["name to track"] == name].index[0]
                datainput.loc[row_index, "notes"] = 'assembly and haplotype found, added to Processing at ENA'
                outtempdf = pd.concat([tempdf, tempdfh])
                procesing_at_ENA_name_list.append(name)
                procesing_at_ENA_haplo_list.append(name)
            else:
                print('exception')


            # join results for name to the main dataframe
            if (outtempdf is None) or outtempdf.empty:
                pass
            else:
                outtempdf = outtempdf.rename(columns={"analysis_alias":"name","first_created":"submission date","bioproject_id":"project",
                                   "analysis_id":"analysis ID", "status_id":"status ID","pipeline_name":"process","state":"status"})
                processing_at_ENA = pd.concat([processing_at_ENA, outtempdf])

    # write out result of function to file
    print(processing_at_ENA)
    processing_at_ENA.to_csv(processing_at_ENA_save_path, sep='\t',index=False)
    datainput.to_csv(in_process_to_submit_savepath, sep='\t',index=False)

    print(len(procesing_at_ENA_name_list), 'assemblies found. Check the Processing_at_ENA output')
    print(len(procesing_at_ENA_haplo_list), 'haplotypes found. Check the Processing_at_ENA output')
    print(procesing_at_ENA_name_list, procesing_at_ENA_haplo_list)
    #return procesing_at_ENA_name_list, procesing_at_ENA_haplo_list #use return statement when running function inside main function

def get_ENA_accession_numbers(procesing_at_ENA_name_list, procesing_at_ENA_haplo_list):
    ##search for assemblies in ENA database
    # have status 4 - move to next step
    # have status 2 - 'In error at ENA'
    global Releasing_Sequences
    Releasing_Sequences = pd.DataFrame(columns=['name', 'submission date', 'accessioned', 'shared to NCBI', 'project',
                                                'analysis ID', 'sample ID', 'GCA ID', 'Contig range', 'Chr range',
                                                'Assembly type', 'status ID', 'Notes'])

    with engine.connect() as conn:  # opens the connection to database

        for name in procesing_at_ENA_name_list:
            # the primary assembly name is not changed
            query_string = f"""select name,created,accessioned,submitted,project_acc,assembly_id,biosample_id,gc_id,contig_acc_range,chromosome_acc_range,assembly_type,status_id 
                                from GCS_ASSEMBLY
                            where name = '{name}'"""
            tempdf = pd.read_sql(query_string, conn, parse_dates={'submitted': '%Y-%m-%d %H:%M:%S'})
            #save the result...
            outtempdf = tempdf.rename(columns={"created": "submission date", "submitted":"shared to NCBI","project_acc": "project",
                         "assembly_id": "analysis ID","biosample_id": "sample ID","gc_id":"GCA ID",
                        "contig_acc_range": "Contig range", "chromosome_acc_range":"Chr range",
                        "assembly_type":"Assembly type" ,"status_id": "status ID"})
            Releasing_Sequences = pd.concat([Releasing_Sequences, outtempdf])

        for name in procesing_at_ENA_haplo_list:
            # the primary assembly name is not changed
            # get the haplotype assembly name to search
            enanamehaplo = name + ' alternate haplotype'
            query_string = f"""select name,created,accessioned,submitted,project_acc,assembly_id,biosample_id,gc_id,contig_acc_range,chromosome_acc_range,assembly_type,status_id 
                                from GCS_ASSEMBLY
                            where name = '{enanamehaplo}'"""
            tempdf = pd.read_sql(query_string, conn, parse_dates={'submitted': '%Y-%m-%d %H:%M:%S'})
            # save the result...
            outtempdf = tempdf.rename(columns={"created": "submission date", "submitted":"shared to NCBI","project_acc": "project",
                         "assembly_id": "analysis ID","biosample_id": "sample ID","gc_id":"GCA ID",
                        "contig_acc_range": "Contig range", "chromosome_acc_range":"Chr range",
                        "assembly_type":"Assembly type" ,"status_id": "status ID"})
            Releasing_Sequences = pd.concat([Releasing_Sequences, outtempdf])
    # save result
    print(len(Releasing_Sequences), 'assemblies looked up and saved to Releasing_Sequences file')
    Releasing_Sequences.to_csv(f'{tracking_file_path}/Releasing_Sequences.txt', sep='\t',index=False)

def check_for_errors(submitted_name_list): #TODO: include step to automate identifying assemblies with error
    # check that both ASSEMBLY_PROCESS and WEBIN_FILE_PROCESS are marked as completed, or report error and add (ERAPRO)
    # check for status 2 vs 4 and relegate to 'in error' if status 2 (ENAPRO)
    # write to the file 'In error at ENA'
    In_error_at_ENA = pd.DataFrame(columns=['name', 'submission date', 'project', 'analysis ID', 'status ID', 'process',
                                            'status', 'error details', 'RT with submitter', 'JIRA', 'Notes'])

    # save result
    In_error_at_ENA.to_csv(f'{tracking_file_path}/In_error_at_ENA.txt', sep='\t')

##################
##  USER INPUT  ##
##################

# set the working directory
# check the current working directory
os.getcwd()  # should be 'C:\\Users\\USERNAME\\pathto\\githubrepo\\ena-content-dataflow' on local machine
# set thw working directory to location of scripts and of config file
os.chdir('scripts/assemblytracking/')
# set which project to track - determines the folder where tracking files will be read and written
project = 'ASG'  # or ASG or ERGA

# set the location of the tracking files
tracking_files_path = f'{project}-tracking-files'

# set the location of the downloaded excel file for reading
exeldl_path = f'{tracking_files_path}/{project} assembly tracking.xlsx'

#set the location of the config file
config_file_path = 'config.yaml'
# TODO: use argparse function

#############################
##  FILE INPUTS + OUTPUTS  ##
#############################
#read in the 0th sheet of excel file, use first row as header
datainput = pd.read_excel(exceldl_path, sheet_name=0, header=1)

# FILE OUTPUTS SAVE LOCATIONS
processing_at_ENA_save_path = f'{tracking_files_path}/processing_at_ENA.txt'
In_error_at_ENA_save_path = f'{tracking_files_path}/In_error_at_ENA.txt'
in_process_to_submit_savepath = f'{tracking_files_path}/In_process_to_submit.txt'
releasing_sequences_savepath = f'{tracking_files_path}/Releasing_Sequences.txt'

#############
##  MAIN   ##
#############
# Purpose of script - automates running of SQL queries and getting SQL results to remove copy paste into SQL cmd step
# from the tracking process

# connect to ERAREAD and check for submitted assemblies - review the In_process_to_submit file to see if any have ERROR
connect_to_database(database='ERA')
check_ERA_for_submission()
# connect to ENA and look up the accessions numbers - review the Releasing_Sequences file to check all are status 4
connect_to_database(database='ENA')
get_ENA_accession_numbers(procesing_at_ENA_name_list, procesing_at_ENA_haplo_list)

#TODO: sort the Releasing Sequences column by name, then by date - requires improvement or addition to indexing
# index / sort levels of the tracking file...
# name
# assembly name (there will be 2 per name) - already indexed by unique assembly name but does include checks for updates
# data completed+ public
Releasing_Sequences = Releasing_Sequences.sort_values(by=['name'])
Releasing_Sequences.groupby(['name'])
Releasing_Sequences = Releasing_Sequences.sort_values(by=['submission date','name'])

#TODO: include step to output list of assemblies with error/private status - check_for_errors(submitted_name_list)

#TODO: add argparse and main functions to run script(s) outside of pycharn direct running
# if __name__ == "main":
# tracking_file_path = f'{project}-tracking-files'  # set the location of the tracking files
# datainput = pd.read_excel(f'{tracking_file_path}/{project} assembly tracking.xlsx', sheet_name=0, header=1)