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

def get_db_creds(database, config_file_path):
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
    logging.basicConfig()
    logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
    engine = create_engine(engine_string)
    print('DONE')
    return engine


def check_era_for_submission(engine, xlinput):
    print(f'checking ERA database for new {project} assembly submissions and status...')

    names_input = xlinput['name to track'].unique()  # read in unique names from excel input file

    pipelite_result = pd.DataFrame(
        columns=['name', 'submission date', 'project', 'analysis ID', 'status ID', 'process', 'status'])
    era_result_name_list = []
    with engine.connect() as conn:  # opens the connection to database
        for name in names_input:  # goes through all the new assemblies added to the tracking file
            # get the term to search (after last .) and then add wildcard
            # NOTE: keep in touch with Sanger about naming conventions!!!
            eraname = None
            tolid_list = name.split('.')
            tolid = tolid_list[0]
            if len(tolid_list) == 2:
                eraname = 'webin-genome-' + tolid + '%'
            elif len(tolid_list) == 3:
                eraname = 'webin-genome-' + tolid + '.' + tolid_list[1] + '%'
            else:
                row_index = xlinput.loc[xlinput["name to track"] == name].index[0]
                xlinput.loc[row_index, "notes"] = 'unexpected name format, not tracked!'

            # look up the tolid in pipelite2_process (expected result 2 rows per assembly)
            #tempdict_list = []
            #with conn.begin():
            #    result2 = conn.execute(text("""select b.analysis_alias, b.first_created, b.bioproject_id, b.analysis_id,
            #    b.status_id, a.pipeline_name, a.state
            #                            from pipelite2_process a
            #                            join analysis b on a.process_id = b.analysis_id
            #                            where analysis_alias like :eraname"""),{"eraname": eraname})
            #for row in result2:
            #    tempdict = {'analysis_alias': row[0], 'first_created': row[1], 'bioproject_id': row[2],
            #                'analysis_id': row[3], 'status_id': row[4], 'pipeline_name': row[5], 'state': row[6]}
            #    tempdict_list.append(tempdict)

            query_string = f"""select b.analysis_alias, b.first_created, b.bioproject_id, b.analysis_id, b.status_id, 
            a.pipeline_name, a.state
                                        from pipelite2_process a
                                        join analysis b on a.process_id = b.analysis_id
                                        where analysis_alias like ('{eraname}')"""
            tempdf = pd.read_sql(query_string, conn, parse_dates={'first_created': '%Y-%m-%d %H:%M:%S'})

            namesfound = tempdf['analysis_alias'].unique()  # gets unique cases bcos 2 rows for each result
            namesfound = list(namesfound)

            # save results to processing at ENA stage (intermediary step)
            tempdf = tempdf.rename(
                columns={"analysis_alias": "name", "first_created": "submission date", "bioproject_id": "project",
                         "analysis_id": "analysis ID", "status_id": "status ID", "pipeline_name": "process",
                         "state": "status"})
            pipelite_result = pd.concat([pipelite_result, tempdf])

            # add notes to the original input dataframe about results of searches
            note = ''
            if len(namesfound) > 0:
                note += 'found '
                for name1 in namesfound:
                    name1 = name1.replace('webin-genome-', '')
                    era_result_name_list.append(name1)
                    note += (name1 + ', ')
            else:
                note += 'no submission found'
            row_index = xlinput.loc[xlinput["name to track"] == name].index[0]
            xlinput.loc[row_index, "notes"] = note

    conn.close()
    print('DONE')

    # filter 1 - remove assembly names that have an error based on 'in error at ena'
    in_error_at_ena = pipelite_result.loc[pipelite_result["status"] != 'COMPLETED']
    in_error_webg = in_error_at_ena['name']
    in_error_webg_list = list(in_error_webg)
    in_error_name_list = []
    for item in in_error_webg_list:
        item = item.replace('webin-genome-', '')
        in_error_name_list.append(item)
    era_result_no_errors = set(era_result_name_list) - set(in_error_name_list)

    in_error_at_ena.to_csv(f'{tracking_files_path}/In_error_at_ENA.txt', sep='\t', index=False)
    print(f'Find assemblies with errors at: {tracking_files_path}/In_error_at_ENA.txt')

    xlinput.to_csv(f'{tracking_files_path}/In_process_to_submit.txt', sep='\t', index=False)
    print(f'In Process to Submit notes added, check the file at: {tracking_files_path}/In_process_to_submit.txt')

    # filter 2 - remove assembly names that are already tracked
    # TODO: consider time delay between haplotype release
    #  need to keep input file list + run again for >1week or add more generic query for untracked against all tolids
    # get list of assembly names already in the tracking file
    tracked_names = pd.read_csv(tracking_file_path, sep='\t', usecols=['name'])
    tracked_names = tracked_names['name'].unique()
    tracked_names = list(tracked_names)

    # filter them against the names found to get only new names for searching in ENA db
    new_names = set(era_result_no_errors) - set(tracked_names)
    print(len(new_names), 'new names found.')

    return new_names

def get_ena_accessions(engine, new_names):
    # TODO: change loop storage from df concatenation to list of dictionary?
    ##search for assemblies in ENA database and get accession numbers
    # have status 4 - move to next step
    # have status 2 - 'In error at ENA'
    releasing_sequences = pd.DataFrame(columns=['name', 'submission date', 'accessioned', 'shared to NCBI', 'project',
                                                'analysis ID', 'sample ID', 'GCA ID', 'Contig range', 'Chr range',
                                                'Assembly type', 'status ID', 'Notes'])

    with engine.connect() as conn:  # opens the connection to database
        print('retrieving assembly sequence accessions from ENAREAD...')

        for name in new_names:
            # tolidlist = name.split('.')
            # tolid = tolidlist[0]
            # name = name + '%'
            # the primary assembly name is not changed
            query_string = f"""select name,created,accessioned,submitted,project_acc,assembly_id,biosample_id,gc_id,
            contig_acc_range,chromosome_acc_range,assembly_type,status_id 
                                from GCS_ASSEMBLY
                            where name = '{name}'"""
            tempdf = pd.read_sql(query_string, conn, parse_dates={'submitted': '%Y-%m-%d %H:%M:%S'})
            # save the result...
            outtempdf = tempdf.rename(
                columns={"created": "submission date", "submitted": "shared to NCBI", "project_acc": "project",
                         "assembly_id": "analysis ID", "biosample_id": "sample ID", "gc_id": "GCA ID",
                         "contig_acc_range": "Contig range", "chromosome_acc_range": "Chr range",
                         "assembly_type": "Assembly type", "status_id": "status ID"})
            releasing_sequences = pd.concat([releasing_sequences, outtempdf])
    conn.close()
    print('DONE')
    # Filter 3 - remove any assemblies that do not have status 4
    not_released = releasing_sequences.loc[releasing_sequences["status ID"] != 4]
    error_append_table = not_released.iloc[:, [0, 1, 4, 5, 11, 10, 7]]
    print(error_append_table)
    in_error = pd.read_csv(f'{tracking_files_path}/In_error_at_ENA.txt', sep='\t')
    in_error_at_ena = pd.concat([in_error, error_append_table])
    in_error_at_ena.to_csv(f'{tracking_files_path}/In_error_at_ENA.txt', sep='\t', index=False)

    # save result
    print(len(releasing_sequences), 'new assemblies with accessions found ready to be added to the tracking file')
    releasing_sequences.to_csv(f'{tracking_files_path}/Releasing_sequences.txt', sep='\t', index=False)
    return releasing_sequences


#############
##  MAIN   ##
#############
# Purpose of script - automate running of SQL queries and getting SQL results to remove copy and paste into SQL cmd step
# from the tracking process

if __name__ == "main":
    # TODO: use argparse function
    # set thw working directory to location of scripts and of config file
    os.chdir('scripts/assemblytracking/')
    # set which project to track - determines the folder where tracking files will be read and written
    project = 'DToL'  # DToL or ASG or ERGA
    # set the location of the tracking files
    tracking_files_path = f'{project}-tracking-files'
    # set the location of the downloaded excel file for reading
    exceldl_path = f'{tracking_files_path}/{project} assembly tracking.xlsx'
    # set the location of the config file
    config_file_path = 'config_private.yaml'
    # FILE OUTPUTS SAVE LOCATIONS
    tracking_file_path = f'{tracking_files_path}/tracking_file.txt'
    # read in the 0th sheet of Excel file, use first row as header
    xlinput = pd.read_excel(exceldl_path, sheet_name=0, header=1)
    xlinput = xlinput.drop(['Unnamed: 3'], axis=1)
    # run functions
    get_db_creds(database='ERA', config_file_path='config_private.yaml')
    check_era_for_submission(engine, xlinput)
    get_db_creds(database='ENA', config_file_path='config_private.yaml')
    get_ena_accessions(engine, new_names)

